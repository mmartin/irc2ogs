#!/usr/bin/env -S python3 -u

import collections
import re
import socket
import socketserver
import sys
import threading
import time
import typing

from datetime import datetime, timedelta

from ogs_api.access_tokens.ui_tokens import get_data
from ogs_api.realtime_api import comm_socket
from ogs_api.realtime_api.chat import chat_join, chat_part, chat_send, pm_send

FAKE_SPACE = '\u2008'

USERNAME_RE = re.compile(r'@"([^"]+)\/[0-9]+"', re.IGNORECASE)

def username(user: dict) -> str:
    ranking = user['ranking']

    if 'provisional' in user['ui_class']:
        rank = '?'
    elif 'professional' in user['ui_class']:
        rank = str(ranking - 36) + 'p'
    else:
        ranking = ranking - 30
        if ranking >= 0:
            rank = str(ranking + 1) + 'd'
        else:
            rank = str(-1 * ranking) + 'k'

    return user['username'].replace(' ', FAKE_SPACE) + '|' + rank

def full_username(user: dict) -> str:
    if 'moderator' in user['ui_class']:
        return '@' + username(user)
    return username(user)

def identity(user: dict) -> str:
    return '%s!%d@online-go.com' % (username(user), user['id'])

class Channel:
    name = None
    topic = None
    refs = 0
    users: typing.Dict[int, dict]

    def __init__(self, name):
        self.name = name
        self.users = dict()

    def join(self, user: dict):
        self.users[user['id']] = user

    def part(self, user: dict):
        del self.users[user['id']]

    def disconnect(self):
        self.users.clear()

class Client(socketserver.BaseRequestHandler):
    nick: str = None

    channels: typing.Set[str]

    _lock = threading.Lock()

    def finish(self):
        self.server.ogs_cleanup(self)
        self.server.clients.remove(self)

    def handle(self):
        self.server.clients.add(self)
        self.channels = set()

        buf = b''
        while True:
            if self.server.socket._closed:
                break

            if not comm_socket.comm_socket.connected:
                time.sleep(1)
                continue

            data: bytes = self.request.recv(1024)
            if not data:
                break

            while True:
                end = data.find(b'\r\n')

                if end != -1:
                    buf += data[:end]
                    self._handle_line(buf.decode())
                    buf = b''
                    data = data[end + 2:]
                else:
                    buf += data
                    break

    def _handle_line(self, line: str):
        print('<', line)
        try:
            cmd, args = line.split(' ', 1)
            args = args.split(' ')
        except ValueError:
            cmd = line
            args = tuple()

        try:
            handler = getattr(self, '_handle_' + cmd.lower())
        except AttributeError:
            print('Handler not found for', cmd)
            handler = None

        if handler is not None:
            handler(*args)

    def _handle_nick(self, nick: str):
        if self.nick is None:
            self.nick = nick
            self._reply(1, ':Welcome to Hell')

        nick = username(get_data()['user']) # TODO: think again.
        self._relay('NICK :' + nick)
        self.nick = nick

        self.server._flush_unread_messages()

    def _handle_ping(self, server: str):
        self._reply(None, 'PONG ' + server)

    def _handle_list(self):
        self._reply(322, '#global-english 42 :Eglish')
        self._reply(322, '#global-help 42 :Help')
        self._reply(322, '#global-offtopic 42 :Offtopic')
        self._reply(322, '#global-supporter 42 :Supporter')
        self._reply(323, ':End of /LIST')

    def _handle_join(self, channels: str):
        for channel in channels.split(','):
            channel_name = channel[1:]
            self.server.ogs_join(channel_name)
            self.channels.add(channel_name)

            self._relay('JOIN ' + channel)

            topic = self.server.channels[channel_name].topic
            if topic:
                self._reply(332, f'{channel} :{topic}')

            self._reply(353, f'= {channel} :{self.nick}')

            for user in self.server.channels[channel_name].users.values():
                self._reply(353, f'= {channel} :' + full_username(user))

            self._reply(366, channel + ' :End of NAMES list')

    def _handle_part(self, channel: str, *args):
        self._relay('PART ' + channel)
        self.server.ogs_part(channel[1:])

    def _handle_privmsg(self, target: str, *args):
        message = ' '.join(args).replace(FAKE_SPACE, ' ')

        if args[0].lower() == ':\x01action':
            message = '/me' + message[8:-1]
        else:
            message = message[1:]

        if target[0] == '#':
            chat_send(target[1:], message)
        else:
            pm_send(target.rsplit('|', 1)[0].replace(FAKE_SPACE, ' '), message)

    def _handle_names(self, channel: str):
        for user in self.server.channels[channel[1:]].users.values():
            self._reply(353, f'= {channel} :' + full_username(user))
        self._reply(366, channel + ' :End of NAMES list')

    def _reply(self, code: int, message: str):
        buf = b':localhost '

        if code is not None:
            buf += str(code).rjust(3, '0').encode()
            buf += b' '

            buf += self.nick.encode()
            buf += b' '

        buf += message.encode()
        self._send(buf)

    def _relay(self, message: str):
        self._send(f':{self.nick} {message}'.encode())

    def _send(self, message: bytes):
        print('>', message)
        with self._lock:
            self.request.send(message)
            self.request.send(b'\r\n')

def server_lock(func):
    def inner(self, *args):
        with self._lock:
            func(self, *args)
    return inner

class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    # TODO: locking?
    channels: typing.Dict[str, Channel] # = dict()
    clients: typing.Set[Client]

    unread_private_messages: typing.List[dict]

    _lock = threading.RLock()

    _join_timeout = None
    _reconnect_timeout = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.channels = dict()
        self.clients = set()
        self.unread_private_messages = list()

        comm_socket.on('chat-message', self._chat_message)
        comm_socket.on('chat-join', self._chat_join)
        comm_socket.on('chat-part', self._chat_part)
        comm_socket.on('chat-topic', self._chat_topic)
        comm_socket.on('private-message', self._private_message)
        comm_socket.on('disconnect', self._on_disconnect)
        comm_socket.on('connect', self._on_connect)

        comm_socket.comm_socket.reconnection = True
        comm_socket.comm_socket.reconnection_attempts = 5
        comm_socket.comm_socket.reconnection_delay = 10
        comm_socket.comm_socket.reconnection_delay_max = 10

        comm_socket.connect()

    def __exit__(self, *args):
        super().__exit__(*args)

        if self._join_timeout is not None:
            self._join_timeout.set()

        if self._reconnect_timeout is not None:
            self._reconnect_timeout.set()

        for client in list(self.clients):
            client.request.shutdown(socket.SHUT_RDWR)
            client.request.close()

        for channel in self.channels.keys():
            self.ogs_part(channel)

        comm_socket.comm_socket._reconnect_abort.set()
        comm_socket.exit()
        comm_socket.comm_socket.disconnect()

    def _on_connect(self, *args, **kwargs):
        if self._reconnect_timeout is not None:
            self._reconnect_timeout.set()
            self._reconnect_timeout = None
            try:
                for channel_name in self.channels.keys():
                    self._ogs_join(channel_name)
            except Exception as e:
                self._die(e)

    def _on_disconnect(self, *args, **kwargs):
        for channel in self.channels.values():
            channel.disconnect()

        max_reconnect_time = (comm_socket.comm_socket.reconnection_attempts + 1) \
                           * comm_socket.comm_socket.reconnection_delay_max

        def _timeout():
            self._reconnect_timeout = threading.Event()
            if not self._reconnect_timeout.wait(max_reconnect_time):
                self._die('Failed to reconnect')
        threading.Thread(target=_timeout, daemon=True).start()

    def _die(self, exception):
        if threading.current_thread() == threading.main_thread():
            raise exception
        else:
            self.shutdown()
            raise exception

    @server_lock
    def _flush_unread_messages(self):
        for message in self.unread_private_messages:
            self._private_message(message)
        self.unread_private_messages.clear()

    def _ogs_join(self, channel_name: str):
        if not comm_socket.comm_socket.connected:
            return

        chat_join(channel_name)

        def _timeout():
            if self._join_timeout is not None:
                return
            event = threading.Event()
            self._join_timeout = event
            max_join_time = len(self.channels) \
                          * comm_socket.comm_socket.reconnection_delay_max
            if not event.wait(max_join_time):
                print('Join timeout reached')
                self._join_timeout = None
                comm_socket.disconnect()
                comm_socket.sleep(comm_socket.comm_socket.reconnection_delay)
                if not self.socket._closed:
                    comm_socket.connect()
                else:
                    self._die(RuntimeError('Dead'))

        threading.Thread(target=_timeout, daemon=True).start()

    def ogs_join(self, channel_name: str):
        if not comm_socket.comm_socket.connected:
            return

        try:
            channel = self.channels[channel_name]
        except KeyError:
            channel = Channel(channel_name)
            self.channels[channel_name] = channel

        if channel.refs == 0:
            self._ogs_join(channel_name)

        channel.refs += 1

    def ogs_part(self, channel_name: str):
        if not comm_socket.comm_socket.connected:
            return

        channel = self.channels[channel_name]

        channel.refs -= 1
        if channel.refs == 0:
            chat_part(channel_name)
            channel.disconnect()

    def ogs_cleanup(self, client: Client):
        for channel_name in client.channels:
            self.ogs_part(channel_name)
        # TODO: anything else?

    @server_lock
    def _chat_message(self, message: dict):
        m = message['message']['m']
        m = USERNAME_RE.sub(lambda m: m.group(1), m)

        if m[:3].lower() == '/me':
            m = '\x01ACTION' + m[3:] + '\x01'

        notify = '@time=%s :%s PRIVMSG #%s :%s' % (
                datetime.utcfromtimestamp(message['message']['t']).isoformat(),
                identity(message),
                message['channel'],
                m)

        for client in self.clients:
            if message['channel'] not in client.channels: continue
            client._send(notify.encode())

    @server_lock
    def _chat_join(self, message: dict):
        channel = message['channel']

        if self._join_timeout is not None:
            self._join_timeout.set()

        for user in message['users']:
            if user['id'] < 0: continue
            ident = identity(user)
            nick = username(user)

            self.channels[channel].join(user)

            join_notify = f':{ident} JOIN #{channel}'

            if user['ui_class'].find('moderator') != -1:
                mode_notify = f':localhost MODE #{channel} +o {nick}'
            else:
                mode_notify = None

            for client in self.clients:
                if client.nick == nick: continue
                if channel not in client.channels: continue
                client._send(join_notify.encode())
                if mode_notify:
                    client._send(mode_notify.encode())

    @server_lock
    def _chat_part(self, message: dict):
        channel = message['channel']

        user = message['user']
        if user['id'] < 0: return

        self.channels[channel].part(user)

        ident = identity(user)

        notify = f':{ident} PART #{channel} :Death comes to us all'
        for client in self.clients:
            if channel not in client.channels: continue
            client._send(notify.encode())

    @server_lock
    def _chat_topic(self, message: dict):
        channel = message['channel']
        topic = message['topic']

        self.channels[channel].topic = topic

        for client in self.clients:
            if channel not in client.channels: continue
            client._reply(332, f'#{channel} :{topic}')

    @server_lock
    def _private_message(self, message: dict):
        m = message['message']['m']

        if m[:3].lower() == '/me':
            m = '\x01ACTION' + m[3:] + '\x01'

        notify = '@time=%s :%%s PRIVMSG %%s :%s' % (
                datetime.utcfromtimestamp(message['message']['t']).isoformat(),
                m)

        if len(self.clients) == 0:
            self.unread_private_messages.append(message)
        else:
            for client in self.clients:
                if message['from']['id'] == get_data()['user']['id']:
                    x = client.nick
                    y = username(message['to'])
                else:
                    x = identity(message['from'])
                    y = client.nick

                client._send((notify % (x, y)).encode())


def main():
    with Server(('localhost', 6667), Client) as server:
        server.serve_forever()

if __name__ == "__main__":
    main()
