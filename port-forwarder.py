#!/usr/bin/env python3
import argparse
import logging
import os
from collections import namedtuple, deque
from select import select
from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, getdefaulttimeout
from threading import Thread, Lock
from typing import List, Tuple, Set, Optional, Union

CHUNK_SIZE_B = 4096  # B
MAX_MEMORY_B = 16*(1024**2)  # 16MiB

PROXY_INFO = namedtuple('ProxyInfo', ['host', 'port', 'tcp'])
PROXY_PAIR = Tuple[PROXY_INFO, PROXY_INFO]  # from, to
ProxyOrPipe = Union["ProxySocket", int]


class ProxySocket(socket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_server: bool = False
        self.info: Optional[PROXY_INFO] = None
        self.proxy_to: Optional[Union[PROXY_INFO, "ProxySocket"]] = None
        self.read_cache_lock = Lock()
        self.read_cache: deque[bytes] = deque()
        self.write_cache: deque[bytes] = deque()

    def accept(self) -> "ProxySocket":
        # noinspection PyUnresolvedReferences
        fd, addr = self._accept()
        sock = ProxySocket(self.family, self.type, self.proto, fileno=fd)
        if getdefaulttimeout() is None and self.gettimeout():
            sock.setblocking(True)
        return sock

    def memory_usage(self) -> int:
        return sum(map(lambda x: len(x), self.read_cache)) + sum(map(lambda x: len(x), self.write_cache))


def create_socket_server(info: PROXY_INFO) -> ProxySocket:
    s = ProxySocket(AF_INET, SOCK_STREAM if info.tcp else SOCK_DGRAM)
    s.is_server = True
    s.info = info
    s.setblocking(False)
    s.bind((info.host, info.port))
    if info.tcp:
        s.listen(10)
    return s


def create_proxy_servers(pairs: List[PROXY_PAIR]) -> List[ProxySocket]:
    servers: List[ProxySocket] = []

    for proxy_from, proxy_to in pairs:
        server = create_socket_server(proxy_from)
        server.proxy_to = proxy_to
        servers.append(server)
    return servers


def create_client_connection(source: ProxySocket, target: PROXY_INFO) -> ProxySocket:
    s = ProxySocket(AF_INET, SOCK_STREAM if target.tcp else SOCK_DGRAM)
    s.connect((target.host, target.port))
    s.proxy_to = source
    return s


def create_local_udp_server() -> ProxySocket:
    return create_socket_server(PROXY_INFO(host='127.0.0.1', port=0, tcp=False))


def run_proxy(pairs: List[PROXY_PAIR], uid: Optional[int] = None, guid: Optional[int] = None) -> None:
    servers = create_proxy_servers(pairs)
    if guid is not None or uid is not None:
        from os import setuid, setgid
        setgid(guid)
        setuid(uid)

    try:
        __run_proxy_loop(servers)
    finally:
        print('stopping servers')
        [s.close() for s in servers]


def __run_proxy_loop(servers: List[ProxySocket]) -> None:
    # this server will be notified when a new connection is established in order to unblock select
    pipe_unblock_select_read, pipe_unblock_select_write = os.pipe()

    all_readers: List[ProxyOrPipe] = servers[:]
    all_readers.append(pipe_unblock_select_read)

    all_writers: List[ProxySocket] = []
    memory_usage = 0

    def unblock_select_wait() -> None:
        os.write(pipe_unblock_select_write, b'1')

    while True:
        possible_readers = all_readers if memory_usage < MAX_MEMORY_B else servers
        logging.debug(f'[MEM] {memory_usage // 1024} / {MAX_MEMORY_B // 1024} kiB used')
        possible_writes = list(filter(lambda x: x.write_cache, all_writers))
        logging.debug(f'[LOOP] possible readers {len(possible_readers)}, writers {len(possible_writes)}')
        if logging.root.level == logging.DEBUG:
            logging.debug(f"[LOOP] readers: {list(map(lambda x: x.fileno(), possible_readers))}")
            logging.debug(f"[LOOP] writers: {list(map(lambda x: x.fileno(), possible_writes))}")
        readers, writers, err = select(possible_readers, possible_writes,
                                       list(set(possible_writes + possible_readers))
                                       )  # type: List[ProxySocket], List[ProxySocket], List[ProxySocket]
        logging.debug(f'[LOOP] selected readers {len(readers)}, writers {len(writers)}, in error {len(err)}')
        dead_sockets: Set[ProxySocket] = set(err)

        for reader in readers:
            if reader in dead_sockets:
                continue
            if reader == pipe_unblock_select_read:
                logging.debug(f'[ACC] unblocking request received')
                os.read(pipe_unblock_select_read, 1)
            elif reader.is_server:
                if reader.info.tcp:
                    logging.debug(f'[ACC] accepting new client to {reader.info}')
                    new_client = reader.accept()
                    all_writers.append(new_client)
                    all_readers.append(new_client)
                    proxy_info = reader.proxy_to
                    logging.debug(f'[ACC] new client accepted: {new_client.fileno()}')

                    def f():
                        try:
                            logging.debug(f'[C] {new_client.fileno()}: creating peer connection')
                            proxy_to = create_client_connection(new_client, proxy_info)
                            logging.debug(f'[C] {new_client.fileno()}: peer connection created as {proxy_to.fileno()}')
                            with new_client.read_cache_lock:
                                proxy_to.write_cache.extend(new_client.read_cache)
                                new_client.proxy_to = proxy_to
                                new_client.read_cache.clear()
                            logging.debug(f'[C] {new_client.fileno()}: first cache copied to {proxy_to.fileno()}')
                            all_readers.append(proxy_to)
                            all_writers.append(proxy_to)

                            unblock_select_wait()
                        except ConnectionError:
                            new_client.close()

                    Thread(target=f, daemon=True).start()
                else:
                    raise NotImplementedError("UDP not yet implemented")
            else:
                try:
                    data = reader.recv(CHUNK_SIZE_B)
                    memory_usage += len(data)
                except ConnectionError:
                    data = bytes(0)
                if not data:
                    dead_sockets.add(reader)
                    memory_usage -= reader.memory_usage()
                    try:
                        reader.proxy_to.close()
                    except OSError:
                        pass
                    dead_sockets.add(reader.proxy_to)
                    memory_usage -= reader.proxy_to.memory_usage()
                else:
                    if reader.proxy_to is not None:
                        reader.proxy_to.write_cache.append(data)
                    else:
                        with reader.read_cache_lock:
                            if reader.proxy_to is not None:
                                reader.proxy_to.write_cache.append(data)
                            else:
                                reader.read_cache.append(data)

        for writer in writers:
            if writer in dead_sockets:
                continue
            if len(writer.write_cache):
                write_bytes = writer.write_cache.popleft()
                try:
                    sent_count = writer.send(write_bytes)
                    if sent_count < len(write_bytes):
                        write_bytes = write_bytes[sent_count:]
                        writer.write_cache.appendleft(write_bytes)
                    memory_usage -= sent_count
                except ConnectionError:
                    dead_sockets.add(writer)
                    memory_usage -= writer.memory_usage()
                    try:
                        writer.proxy_to.close()
                    except OSError:
                        pass
                    dead_sockets.add(writer.proxy_to)
                    memory_usage -= writer.proxy_to.memory_usage()

        for dead in dead_sockets:
            try:
                all_writers.remove(dead)
            except ValueError:
                pass
            try:
                all_readers.remove(dead)
            except ValueError:
                pass


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Runs a proxy from point A to point B')
    parser.add_argument('local_ip', metavar='localIP', type=str, help='an IP address to bind')
    parser.add_argument('local_port', metavar='localPort', type=int, help='a port to bind')
    parser.add_argument('remote_ip', metavar='remoteIP', type=str, help='an IP address to proxy to')
    parser.add_argument('remote_port', metavar='remotePort', type=int, help='a port to proxy to')
    parser.add_argument('--user', metavar='user', type=str, help='change uid to user', default='')
    parser.add_argument('--group', metavar='group', type=str, help='change guid to user', default='')
    parser.add_argument('--udp', dest='udp', action='store_const', const=True, default=False,
                        help='use UDP instead of TCP')
    args = parser.parse_args()

    uid: Optional[int] = None
    guid: Optional[int] = None
    if args.user or args.group:
        try:
            # noinspection PyUnresolvedReferences
            import grp
            # noinspection PyUnresolvedReferences
            import pwd
            if args.user:
                uid = pwd.getpwnam(args.user).pw_uid
            if args.group:
                guid = grp.getgrnam(args.group).gr_gid
        except ModuleNotFoundError:
            print("Cannot determine user's UID / GUID")
            return 1

    run_proxy([(
        PROXY_INFO(host=args.local_ip, port=args.local_port, tcp=not args.udp),
        PROXY_INFO(host=args.remote_ip, port=args.remote_port, tcp=not args.udp)
    )], uid, guid)
    return 0


if __name__ == '__main__':
    try:
        exit(main())
    except KeyboardInterrupt:
        print("exiting")
        exit(2)
