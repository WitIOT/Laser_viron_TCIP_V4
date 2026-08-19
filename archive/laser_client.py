# laser_client.py
from __future__ import annotations
import socket
import threading

class LaserClient:
    """Simple TCP client for the laser controller (telnet-like)."""

    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.lock = threading.Lock()

    def connect(self) -> None:
        self.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect((self.host, self.port))
        self.sock = s

    def close(self) -> None:
        with self.lock:
            if self.sock:
                try:
                    self.sock.close()
                except Exception:
                    pass
                self.sock = None

    def send_cmd(self, cmd: str) -> str:
        """Send a command like '$FIRE' and return one-line response (ending with \\n)."""
        with self.lock:
            if not self.sock:
                raise RuntimeError("Not connected")
            data = (cmd.strip() + "\n").encode()
            self.sock.sendall(data)
            self.sock.settimeout(self.timeout)
            chunks = []
            try:
                while True:
                    b = self.sock.recv(1024)
                    if not b:
                        break
                    chunks.append(b)
                    if b.endswith(b"\n"):
                        break
            except socket.timeout:
                pass
            return b"".join(chunks).decode(errors="ignore").strip()
