#!/usr/bin/env python3
import argparse
import logging
import os
import time
import platform
from collections import namedtuple, deque
from select import select
from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, getdefaulttimeout
from threading import Thread, Lock
from typing import List, Tuple, Set, Optional, Union, Dict

CHUNK_SIZE_B = 4096  # B
MAX_MEMORY_B = 16 * (1024 ** 2)  # 16MiB
SOCKET_IDLE_TIMEOUT = 120  # s, 0 to disabled
SOCKET_IDLE_TCP_TIMEOUT_ENABLED = False

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
        self.last_used = time.time()

    def accept(self) -> "ProxySocket":
        # noinspection PyUnresolvedReferences
        fd, addr = self._accept()
        sock = ProxySocket(self.family, self.type, self.proto, fileno=fd)
        if getdefaulttimeout() is None and self.gettimeout():
            sock.setblocking(True)
        return sock

    def memory_usage(self) -> int:
        return sum(map(lambda x: len(x), self.read_cache)) + sum(map(lambda x: len(x), self.write_cache))


def create_socket_server(info: PROXY_INFO, wait_for_port = False) -> ProxySocket:
    s = ProxySocket(AF_INET, SOCK_STREAM if info.tcp else SOCK_DGRAM)
    s.is_server = True
    s.info = info
    s.setblocking(False)
    while True:
        try:
            s.bind((info.host, info.port))
            break
        except OSError:
            if wait_for_port:
                logging.info(f'[SERV] {info}\'s port is not ready to be taken, retrying in 5 seconds')
                time.sleep(5)
            else:
                raise
    if info.tcp:
        s.listen(10)
    return s


def create_proxy_servers(pairs: List[PROXY_PAIR], wait_for_ports=False) -> List[ProxySocket]:
    servers: List[ProxySocket] = []

    for proxy_from, proxy_to in pairs:
        server = create_socket_server(proxy_from, wait_for_ports)
        server.proxy_to = proxy_to
        servers.append(server)
    return servers


def create_client_connection(source: Union[ProxySocket, PROXY_INFO], target: PROXY_INFO) -> ProxySocket:
    s = ProxySocket(AF_INET, SOCK_STREAM if target.tcp else SOCK_DGRAM)
    if target.tcp:
        s.connect((target.host, target.port))
    s.proxy_to = source
    return s


def run_proxy(pairs: List[PROXY_PAIR],
              uid: Optional[int] = None,
              guid: Optional[int] = None,
              wait_for_port=False) -> None:
    servers = create_proxy_servers(pairs, wait_for_port)
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
    os_windows = platform.system() == 'Windows'
    # this server will be notified when a new connection is established in order to unblock select
    if os_windows:
        pipe_unblock_select_read = create_socket_server(PROXY_INFO(host='127.0.0.1', port=0, tcp=False))
        pipe_unblock_select_write = socket(AF_INET, SOCK_DGRAM)
    else:
        pipe_unblock_select_read, pipe_unblock_select_write = os.pipe()

    all_readers: List[ProxyOrPipe] = []

    all_writers: List[ProxySocket] = []

    data_memory_usage = 0

    udp_sockets: Dict[Tuple[str, int], ProxySocket] = {}

    last_time_inactive_removed = time.time()

    def can_open_more_sockets() -> bool:
        return max(len(all_readers) + len(servers), len(all_writers)) < (500 if os_windows else 1000)

    def unblock_select_wait() -> None:
        if os_windows:
            pipe_unblock_select_write.sendto(b'1', pipe_unblock_select_read.getsockname())
        else:
            os.write(pipe_unblock_select_write, b'1')

    while True:
        dead_sockets: Set[ProxySocket] = set()

        if SOCKET_IDLE_TIMEOUT and time.time() - last_time_inactive_removed > min(180, SOCKET_IDLE_TIMEOUT):
            last_time_inactive_removed = time.time()
            logging.debug('[MEM] removing inactive sockets')
            sockets = all_readers if SOCKET_IDLE_TCP_TIMEOUT_ENABLED else udp_sockets.values()
            for s in sockets:
                if type(s) is int or s.is_server or time.time() - s.last_used < SOCKET_IDLE_TIMEOUT:
                    continue
                dead_sockets.add(s)
                if isinstance(s.proxy_to, ProxySocket):
                    dead_sockets.add(s)

        memory_usage = data_memory_usage + len(all_readers) * 1360

        if memory_usage < MAX_MEMORY_B:
            possible_readers = all_readers + [pipe_unblock_select_read]
            if can_open_more_sockets():
                possible_readers.extend(servers)
            else:
                logging.warning("[MEM] To much opened sockets, cannot accept more")
        else:
            possible_readers = []

        logging.debug(f'[MEM] {memory_usage // 1024} / {MAX_MEMORY_B // 1024} kiB used')
        possible_writes = list(filter(lambda x: x.write_cache, all_writers))
        logging.debug(f'[LOOP] possible readers {len(possible_readers)}, writers {len(possible_writes)}')
        try:
            readers, writers, err = select(possible_readers, possible_writes,
                                           list(set(possible_writes + possible_readers))
                                           )  # type: List[ProxySocket], List[ProxySocket], List[ProxySocket]
            logging.debug(f'[LOOP] selected readers {len(readers)}, writers {len(writers)}, in error {len(err)}')
            dead_sockets.update(err)
        except ValueError:
            readers = writers = []
            logging.debug('[LOOP] invalid socket found, removing')
            for s in filter(lambda x: isinstance(x, ProxySocket) and x.fileno() < 0, all_readers):  # type: ProxySocket
                dead_sockets.add(s)
                if isinstance(s.proxy_to, ProxySocket):
                    dead_sockets.add(s.proxy_to)

        for reader in readers:
            if reader in dead_sockets:
                continue
            if reader == pipe_unblock_select_read:
                logging.debug(f'[ACC] unblocking request received')
                if os_windows:
                    pipe_unblock_select_read.recvfrom(1)
                else:
                    os.read(pipe_unblock_select_read, 1)
            elif reader.is_server:
                if not can_open_more_sockets():
                    logging.warning("[ACC] to much opened sockets, rejecting")
                    continue
                if reader.info.tcp:
                    logging.debug(f'[ACC] accepting new client to {reader.info}')
                    try:
                        new_client = reader.accept()
                    except (BlockingIOError, ConnectionError, OSError):
                        continue
                    all_writers.append(new_client)
                    all_readers.append(new_client)
                    proxy_info = reader.proxy_to
                    logging.debug(f'[ACC] new client accepted: {new_client.fileno()} {new_client.getpeername()}')

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
                    try:
                        data, address = reader.recvfrom(CHUNK_SIZE_B)
                    except (BlockingIOError, OSError, ConnectionError):
                        continue
                    target = reader.proxy_to
                    if address not in udp_sockets:
                        soc = create_client_connection(
                            PROXY_INFO(host=address[0], port=address[1], tcp=False),
                            target
                        )
                        udp_sockets[address] = soc
                        all_readers.append(soc)
                    else:
                        soc = udp_sockets[address]
                    try:
                        soc.sendto(data, (target.host, target.port))
                    except (BlockingIOError, OSError, ConnectionError):
                        logging.debug(f'[KILL] UDP send failed {reader.fileno()}')
                        dead_sockets.add(soc)
            else:
                reader.last_used = time.time()
                try:
                    data = reader.recv(CHUNK_SIZE_B)
                    data_memory_usage += len(data)
                except (BlockingIOError, OSError, ConnectionError):
                    data = bytes(0)
                if not data:
                    logging.debug(f'[KILL] no data read or error from {reader.fileno()}')
                    dead_sockets.add(reader)
                    data_memory_usage -= reader.memory_usage()
                    if isinstance(reader.proxy_to, ProxySocket):
                        dead_sockets.add(reader.proxy_to)
                        data_memory_usage -= reader.proxy_to.memory_usage()
                else:
                    if reader.proxy_to is not None:
                        if isinstance(reader.proxy_to, ProxySocket):
                            reader.proxy_to.write_cache.append(data)
                        else:
                            reader.sendto(data, (reader.proxy_to.host, reader.proxy_to.port))
                            data_memory_usage -= len(data)
                    else:
                        with reader.read_cache_lock:
                            if reader.proxy_to is not None:
                                reader.proxy_to.write_cache.append(data)
                            else:
                                reader.read_cache.append(data)

        for writer in writers:
            if writer in dead_sockets:
                continue
            writer.last_used = time.time()
            if len(writer.write_cache):
                write_bytes = writer.write_cache.popleft()
                try:
                    sent_count = writer.send(write_bytes)
                    if sent_count < len(write_bytes):
                        write_bytes = write_bytes[sent_count:]
                        writer.write_cache.appendleft(write_bytes)
                    data_memory_usage -= sent_count
                except (BlockingIOError, OSError, ConnectionError):
                    logging.debug(f'[KILL] write error for {writer.fileno()}')
                    dead_sockets.add(writer)
                    data_memory_usage -= writer.memory_usage()
                    if isinstance(writer.proxy_to, ProxySocket):
                        dead_sockets.add(writer.proxy_to)
                        data_memory_usage -= writer.proxy_to.memory_usage()

        for dead in dead_sockets:
            logging.debug(f"[KILL] killing {dead.fileno()}")
            dead.read_cache.clear()
            dead.write_cache.clear()
            try:
                dead.close()
            except (OSError, ConnectionError):
                pass
            try:
                all_writers.remove(dead)
            except ValueError:
                pass
            try:
                all_readers.remove(dead)
            except ValueError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description='Runs a proxy from point A to point B')
    parser.add_argument('local_ip', metavar='localIP', type=str, help='an IP address to bind')
    parser.add_argument('local_port', metavar='localPort', type=int, help='a port to bind')
    parser.add_argument('remote_ip', metavar='remoteIP', type=str, help='an IP address to proxy to')
    parser.add_argument('remote_port', metavar='remotePort', type=int, help='a port to proxy to')
    parser.add_argument('--user', metavar='user', type=str, help='change uid to user', default='')
    parser.add_argument('--group', metavar='group', type=str, help='change guid to user', default='')
    parser.add_argument('--udp', dest='udp', action='store_const', const=True, default=False,
                        help='use UDP instead of TCP')
    parser.add_argument('--debug', dest='debug', action='store_const', const=True, default=False,
                        help='be more verbose')
    parser.add_argument('--wait-for-port', dest='portWait', action='store_const', const=True, default=False,
                        help='be more verbose')
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

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    run_proxy([(
        PROXY_INFO(host=args.local_ip, port=args.local_port, tcp=not args.udp),
        PROXY_INFO(host=args.remote_ip, port=args.remote_port, tcp=not args.udp)
    )], uid=uid, guid=guid, wait_for_port=args.portWait)
    return 0


if __name__ == '__main__':
    try:
        exit(main())
    except KeyboardInterrupt:
        print("exiting")
        exit(2)
