import argparse
from collections import namedtuple, deque
from select import select
from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, getdefaulttimeout
from typing import List, Tuple, Set, Optional, Union

PROXY_INFO = namedtuple('ProxyInfo', ['host', 'port', 'tcp'])
PROXY_PAIR = Tuple[PROXY_INFO, PROXY_INFO]  # from, to


class ProxySocket(socket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_server: bool = False
        self.proxy_to: Optional[Union[PROXY_INFO, "ProxySocket"]] = None
        self.write_cache: deque[bytes] = deque()

    def accept(self) -> "ProxySocket":
        # noinspection PyUnresolvedReferences
        fd, addr = self._accept()
        sock = ProxySocket(self.family, self.type, self.proto, fileno=fd)
        if getdefaulttimeout() is None and self.gettimeout():
            sock.setblocking(True)
        return sock


def create_socket_server(info: PROXY_INFO) -> ProxySocket:
    s = ProxySocket(AF_INET, SOCK_STREAM if info.tcp else SOCK_DGRAM)
    s.is_server = True
    s.setblocking(False)
    s.bind((info.host, info.port))
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


def run_proxy(pairs: List[PROXY_PAIR]) -> None:
    servers = create_proxy_servers(pairs)

    all_readers = servers[:]
    all_writers = []

    while True:
        readers, writers, _ = select(all_readers, all_writers,
                                     [])  # type: List[ProxySocket], List[ProxySocket], List[None]
        dead_sockets: Set[ProxySocket] = set()

        for reader in readers:
            if reader.is_server:
                new_client = reader.accept()
                try:
                    new_client.proxy_to = create_client_connection(new_client, reader.proxy_to)
                    to_add = [new_client, new_client.proxy_to]
                    all_readers.extend(to_add)
                    all_writers.extend(to_add)
                    del to_add
                except ConnectionRefusedError:
                    new_client.close()
                del new_client
            else:
                data = reader.recv(4096)
                if not data:
                    dead_sockets.add(reader)
                    reader.proxy_to.close()
                    dead_sockets.add(reader.proxy_to)
                else:
                    reader.proxy_to.write_cache.append(data)
                del data
        for writer in writers:
            if len(writer.write_cache):
                write_bytes = writer.write_cache.popleft()
                sent_count = writer.send(write_bytes)
                if sent_count < len(write_bytes):
                    write_bytes = write_bytes[sent_count:]
                    writer.write_cache.appendleft(write_bytes)
                del write_bytes, sent_count
        for dead in dead_sockets:
            all_writers.remove(dead)
            all_readers.remove(dead)


def main():
    parser = argparse.ArgumentParser(description='Runs a proxy from point A to point B')
    parser.add_argument('local_ip', metavar='localIP', type=str, help='an IP address to bind')
    parser.add_argument('local_port', metavar='localPort', type=int, help='a port to bind')
    parser.add_argument('remote_ip', metavar='remoteIP', type=str, help='an IP address to proxy to')
    parser.add_argument('remote_port', metavar='remotePort', type=int, help='a port to proxy to')
    parser.add_argument('--udp', dest='udp', action='store_const', const=True, default=False,
                        help='use UDP instead of TCP')
    args = parser.parse_args()
    run_proxy([(
        PROXY_INFO(host=args.local_ip, port=args.local_port, tcp=True),
        PROXY_INFO(host=args.remote_ip, port=args.remote_port, tcp=True)
    )])


if __name__ == '__main__':
    main()
