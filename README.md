# Port Forwarder

Simple port forwarding from point A to point B, both for tcp and udp and both for Windows and Linux.

## Usage

```shell
usage: port-forwarder.py [-h] [--user user] [--group group] [--udp] [--debug]
                         [--wait-for-port]
                         localIP localPort remoteIP remotePort

Runs a proxy from point A to point B

positional arguments:
  localIP          an IP address to bind
  localPort        a port to bind
  remoteIP         an IP address to proxy to
  remotePort       a port to proxy to

optional arguments:
  -h, --help       show this help message and exit
  --user user      change uid to user
  --group group    change guid to user
  --udp            use UDP instead of TCP
  --debug          be more verbose
  --wait-for-port  if the port is taken now, wait until it becomes available
```

## Example

```shell
./port-forwarder.py 127.0.0.1 8080 10.1.1.25 80
```

will forward from `127.0.0.1:8080` to `10.1.1.25:80` tcp.