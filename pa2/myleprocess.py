"""
myleprocess.py -- CS158A PA2: Leader Election on an asynchronous ring.

Implements the Chang-Roberts leader election algorithm over a ring of
TCP-connected processes. Each process has exactly two neighbors:
  - a SERVER role: binds/listens/accepts a connection from the neighbor
    that will act as a client toward us. We RECEIVE messages on this
    connection.
  - a CLIENT role: connects out to the other neighbor's server. We SEND
    messages on this connection.

Messages always flow client -> server, so the ring's message direction
is fixed by who connects to whom (see config.txt).

Usage:
    python3 myleprocess.py [config_file] [log_file] [--wait]

    config_file  path to the config file (default: config.txt)
    log_file     path to the log file to write (default: log.txt)
    --wait       pause with an interactive prompt between starting the
                 server and connecting as a client, so all processes in
                 a classroom demo can be started before anyone connects.
"""

import json
import socket
import sys
import threading
import time
import uuid as uuid_lib
from datetime import datetime


class Message:
    """Election message exchanged between neighbors, serialized as JSON."""

    def __init__(self, uuid, flag):
        self.uuid = uuid  # uuid.UUID of the candidate/leader
        self.flag = flag  # 0 = still electing, 1 = leader already elected

    def to_json(self):
        return json.dumps({"uuid": str(self.uuid), "flag": self.flag})

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return Message(uuid_lib.UUID(obj["uuid"]), int(obj["flag"]))


class LeaderElectionNode:
    def __init__(self, config_path="config.txt", log_path="log.txt", interactive_wait=False):
        self.config_path = config_path
        self.log_path = log_path
        self.interactive_wait = interactive_wait

        self.my_id = uuid_lib.uuid4()
        self.leader_id = None
        self.state = 0  # 0 = still electing, 1 = leader known

        self.server_ip = None
        self.server_port = None
        self.neighbor_ip = None
        self.neighbor_port = None

        self.recv_conn = None  # accepted socket (server side) -- we receive here
        self.send_conn = None  # connected socket (client side) -- we send here

        self._log_lock = threading.Lock()
        self._server_ready = threading.Event()

        self._read_config()
        self._init_log()

    # ---- setup -----------------------------------------------------

    def _read_config(self):
        with open(self.config_path) as f:
            lines = [line.strip() for line in f if line.strip()]
        self.server_ip, port_str = [p.strip() for p in lines[0].split(",")]
        self.server_port = int(port_str)
        self.neighbor_ip, port_str2 = [p.strip() for p in lines[1].split(",")]
        self.neighbor_port = int(port_str2)

    def _init_log(self):
        with open(self.log_path, "w"):
            pass

    def log(self, line):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")
        with self._log_lock:
            with open(self.log_path, "a") as f:
                f.write(f"[{timestamp}] {line}\n")

    # ---- connection setup (server thread + client connect) ---------

    def _run_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.server_ip, self.server_port))
        srv.listen(1)
        print(f"[server] listening on {self.server_ip}:{self.server_port}")
        conn, addr = srv.accept()  # blocks until the other neighbor connects
        print(f"[server] accepted connection from {addr}")
        self.recv_conn = conn
        self._server_ready.set()

    def _connect_client(self):
        while True:
            try:
                cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cli.connect((self.neighbor_ip, self.neighbor_port))
                self.send_conn = cli
                print(f"[client] connected to {self.neighbor_ip}:{self.neighbor_port}")
                return
            except (ConnectionRefusedError, OSError):
                # neighbor's server may not be up yet -- retry until it is
                time.sleep(1)

    # ---- messaging ---------------------------------------------------

    def _send_message(self, msg):
        data = (msg.to_json() + "\n").encode("utf-8")
        self.send_conn.sendall(data)
        self.log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

    def _recv_message(self, reader):
        line = reader.readline()
        if not line:
            return None
        return Message.from_json(line.decode("utf-8").strip())

    # ---- main algorithm ----------------------------------------------

    def start(self):
        print(f"My id: {self.my_id}")
        self.log(f"My id: {self.my_id}")

        # Run accept() on its own thread so it can block while we also
        # connect() as a client -- doing both sequentially on one thread
        # would deadlock every node in the ring at once.
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()

        if self.interactive_wait:
            input("Press Enter when everyone is ready to connect...")

        self._connect_client()
        self._server_ready.wait()
        server_thread.join()

        # Initial message: send our own id with no comparison. Happens once.
        self._send_message(Message(self.my_id, 0))

        # From here on a single (the main) thread is enough: we just
        # block on recv, compare, and forward/ignore.
        reader = self.recv_conn.makefile("rb")

        while True:
            msg = self._recv_message(reader)
            if msg is None:
                break

            if msg.uuid > self.my_id:
                relation = "greater"
            elif msg.uuid < self.my_id:
                relation = "less"
            else:
                relation = "same"

            state_str = str(self.state) if self.state == 0 else f"1, leader={self.leader_id}"
            self.log(f"Received: uuid={msg.uuid}, flag={msg.flag}, {relation}, {state_str}")

            if msg.flag == 1:
                # Leader announcement circulating the ring. Record it,
                # relay it once so the next node also learns it, then
                # this node's job is done.
                self.leader_id = msg.uuid
                self.state = 1
                print(f"Leader is decided to {self.leader_id}.")
                self.log(f"Leader is decided to {self.leader_id}.")
                self._send_message(Message(self.leader_id, 1))
                break

            # flag == 0: still electing
            if msg.uuid > self.my_id:
                self._send_message(Message(msg.uuid, 0))
            elif msg.uuid < self.my_id:
                self.log(f"Ignored: uuid={msg.uuid} is less than mine.")
            else:
                # Our own id circulated the whole ring back to us: we win.
                self.leader_id = self.my_id
                self.state = 1
                print(f"Leader is decided to {self.leader_id}.")
                self.log(f"Leader is decided to {self.leader_id}.")
                self._send_message(Message(self.leader_id, 1))
                break

        print(f"leader is {self.leader_id}")
        self.log(f"leader is {self.leader_id}")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--wait"]
    config_file = args[0] if len(args) > 0 else "config.txt"
    log_file = args[1] if len(args) > 1 else "log.txt"
    interactive = "--wait" in sys.argv

    node = LeaderElectionNode(config_file, log_file, interactive_wait=interactive)
    node.start()
