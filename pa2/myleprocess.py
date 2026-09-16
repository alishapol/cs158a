# myleprocess.py
# CS158A PA2 - Leader Election on a ring
#
# Each process connects to two neighbors:
#   - it runs a server and waits for one neighbor to connect to it (this is
#     how it receives messages)
#   - it also connects out to the other neighbor as a client (this is how
#     it sends messages)
#
# Run it like this:
#   python3 myleprocess.py config.txt log.txt
# Add --wait if you want to pause before connecting out, so everyone in
# the ring has time to start their server first.

import json
import socket
import sys
import threading
import time
import uuid as uuid_lib
from datetime import datetime


class Message:
    # what gets sent over the socket, turned into JSON
    def __init__(self, uuid, flag):
        self.uuid = uuid
        self.flag = flag

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

        # my own random id, this stays the same the whole time
        self.my_id = uuid_lib.uuid4()
        self.leader_id = None
        # 0 = still voting, 1 = we know who the leader is
        self.state = 0

        self.server_ip = None
        self.server_port = None
        self.neighbor_ip = None
        self.neighbor_port = None

        # socket we accept as a server, this is where we read messages from
        self.recv_conn = None
        # socket we open as a client, this is where we write messages to
        self.send_conn = None

        self._log_lock = threading.Lock()
        self._server_ready = threading.Event()
        # holds bytes we've read off the socket but haven't used yet
        self._recv_buffer = b""

        self._read_config()
        self._init_log()

    def _read_config(self):
        # first line = my own ip/port to listen on
        # second line = the neighbor i connect out to
        with open(self.config_path) as f:
            lines = [line.strip() for line in f if line.strip()]
        self.server_ip, port_str = [p.strip() for p in lines[0].split(",")]
        self.server_port = int(port_str)
        self.neighbor_ip, port_str2 = [p.strip() for p in lines[1].split(",")]
        self.neighbor_port = int(port_str2)

    def _init_log(self):
        # start with an empty log file
        with open(self.log_path, "w"):
            pass

    def log(self, line):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")
        with self._log_lock:
            with open(self.log_path, "a") as f:
                f.write(f"[{timestamp}] {line}\n")

    def _run_server(self):
        # this waits for the other neighbor to connect to us
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.server_ip, self.server_port))
        srv.listen(1)
        print(f"[server] listening on {self.server_ip}:{self.server_port}")
        # this line blocks until someone connects
        conn, addr = srv.accept()
        print(f"[server] accepted connection from {addr}")
        self.recv_conn = conn
        self._server_ready.set()

    def _connect_client(self):
        # keep trying in case the neighbor's server isn't up yet
        while True:
            try:
                cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cli.connect((self.neighbor_ip, self.neighbor_port))
                self.send_conn = cli
                print(f"[client] connected to {self.neighbor_ip}:{self.neighbor_port}")
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(1)

    def _send_message(self, msg):
        # the json itself already ends in "}", that's our end of message marker
        data = msg.to_json().encode("utf-8")
        self.send_conn.sendall(data)
        self.log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

    def _recv_message(self, sock):
        # tcp is just a stream of bytes, so we keep reading until we see
        # the closing "}" that marks the end of one message
        while b"}" not in self._recv_buffer:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            self._recv_buffer += chunk

        end = self._recv_buffer.index(b"}")
        raw = self._recv_buffer[: end + 1]
        # keep whatever came after the "}" for the next message
        self._recv_buffer = self._recv_buffer[end + 1 :]
        return Message.from_json(raw.decode("utf-8"))

    def start(self):
        print(f"My id: {self.my_id}")
        self.log(f"My id: {self.my_id}")

        # accept() has to run on its own thread, otherwise every node in
        # the ring would be stuck waiting for accept() and nobody would
        # ever get to connect()
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()

        if self.interactive_wait:
            input("Press Enter when everyone is ready to connect...")

        self._connect_client()
        self._server_ready.wait()
        server_thread.join()

        # send my own id once, no comparison needed for this first message
        self._send_message(Message(self.my_id, 0))

        # once both connections are up, one thread is all we need,
        # just read a message, decide what to do, repeat
        while True:
            msg = self._recv_message(self.recv_conn)
            if msg is None:
                break

            # figure out how the incoming id compares to mine, just for the log
            if msg.uuid > self.my_id:
                relation = "greater"
            elif msg.uuid < self.my_id:
                relation = "less"
            else:
                relation = "same"

            state_str = str(self.state) if self.state == 0 else f"1, leader={self.leader_id}"
            self.log(f"Received: uuid={msg.uuid}, flag={msg.flag}, {relation}, {state_str}")

            if msg.flag == 1:
                # someone already found the leader, pass the news along
                # once and then i'm done
                self.leader_id = msg.uuid
                self.state = 1
                print(f"Leader is decided to {self.leader_id}.")
                self.log(f"Leader is decided to {self.leader_id}.")
                self._send_message(Message(self.leader_id, 1))
                break

            # still electing at this point
            if msg.uuid > self.my_id:
                # someone else has a better shot at being leader, pass it on
                self._send_message(Message(msg.uuid, 0))
            elif msg.uuid < self.my_id:
                # my id beats theirs, drop their message
                self.log(f"Ignored: uuid={msg.uuid} is less than mine.")
            else:
                # this is my own id, it made it all the way around the ring
                # so i'm the leader
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
