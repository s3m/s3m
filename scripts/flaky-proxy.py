#!/usr/bin/env python3
"""HTTP CONNECT proxy that breaks the first tunnel partway through a request.

    scripts/flaky-proxy.py <port file> <state file> [break after bytes]

Used by scripts/provider-scenarios.sh to show that s3m retries an upload after a real
network failure. Listens on 127.0.0.1 on a free port (written to <port file>), tunnels
CONNECT requests unchanged (TLS stays end to end), and closes the FIRST tunnel once the
client has sent <break after bytes> (default 65536) through it: to the client this is a
connection dropped in the middle of an upload. Later tunnels pass untouched. <state file>
gets one line per tunnel: "broken <host:port>" once the first tunnel is cut, or
"passed <host:port>" when a later one opens.
"""

import socket
import sys
import threading

port_file, state_file = sys.argv[1], sys.argv[2]
break_after = int(sys.argv[3]) if len(sys.argv) > 3 else 65536
lock = threading.Lock()
tunnels = 0


def record(line):
    with lock, open(state_file, "a", encoding="utf-8") as state:
        state.write(line + "\n")


def pipe(source, target, limit=None):
    """Copy until EOF; with a limit, stop after that many bytes and return True."""
    sent = 0
    try:
        while True:
            data = source.recv(65536)
            if not data:
                return False
            if limit is not None and sent + len(data) >= limit:
                target.sendall(data[: limit - sent])
                return True
            target.sendall(data)
            sent += len(data)
    except OSError:
        return False


def close(*sockets):
    for sock in sockets:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


def handle(client):
    global tunnels
    try:
        request = b""
        while b"\r\n\r\n" not in request:
            data = client.recv(4096)
            if not data:
                return
            request += data
        line = request.split(b"\r\n", 1)[0].decode("latin-1").split()
        if len(line) != 3 or line[0] != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            return
        host, _, port = line[1].rpartition(":")
        upstream = socket.create_connection((host, int(port)), timeout=30)
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        with lock:
            tunnels += 1
            first = tunnels == 1
        back = threading.Thread(target=pipe, args=(upstream, client), daemon=True)
        back.start()
        if first and pipe(client, upstream, break_after):
            record(f"broken {line[1]}")
            close(client, upstream)
            return
        if not first:
            # Recorded when the tunnel opens: the caller may stop the proxy as soon as
            # its client exits.
            record(f"passed {line[1]}")
            pipe(client, upstream)
        back.join(timeout=60)
        close(client, upstream)
    except OSError:
        close(client)


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(16)
    with open(port_file, "w", encoding="utf-8") as out:
        out.write(str(server.getsockname()[1]))
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


main()
