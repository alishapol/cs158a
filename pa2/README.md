# PA2 — Leader Election on an Asynchronous Ring

Implements the Chang–Roberts leader election algorithm over a ring of
TCP-connected processes, using randomly generated UUIDs as process IDs.

## Files

- `myleprocess.py` — the process implementation (client + server roles,
  `Message` class, election logic, logging).
- `config.txt` — this node's config: line 1 is the IP/port this node
  listens on as a server; line 2 is the neighbor's IP/port this node
  connects to as a client.
- `config2.txt`, `config3.txt` — extra config files used only for the
  local 3-process demo below (so three instances can run on one
  machine on different ports).
- `log1.txt`, `log2.txt`, `log3.txt` — logs from the three processes in
  the local demo run.

## How the ring direction works

Each process has two neighbors:

- **Server role**: `bind()` / `listen()` / `accept()` on the IP:port in
  line 1 of the config file. Whoever connects to us as a client is the
  neighbor we **receive** messages from.
- **Client role**: `connect()` to the IP:port in line 2 of the config
  file. This is the neighbor we **send** messages to.

So the ring's message direction is determined entirely by which node's
client connects to which node's server.

`accept()` and `connect()` run on separate threads at startup (server
thread does `bind`/`listen`/`accept`; the main thread does `connect`)
to avoid every node in the ring deadlocking on `accept()` at once.
Once both connections are established, a single thread handles the
rest (blocking `recv`, compare, forward/ignore).

## Running a single node

```bash
python3 myleprocess.py config.txt log.txt
```

Optional args: `python3 myleprocess.py <config_file> <log_file> [--wait]`.
`--wait` inserts an interactive prompt between starting the server and
connecting as a client, useful for a classroom demo so every node can
be started before anyone tries to connect:

```bash
python3 myleprocess.py config.txt log.txt --wait
```

Defaults are `config.txt` and `log.txt` if omitted.

## Local 3-node demo

Three copies of the same script were run on `localhost` with different
ports, forming a ring `node1 -> node2 -> node3 -> node1`
(client of node*N* connects to the server of node*N+1*):

| node | server (recv) | client connects to (send) | config file |
|------|----------------|----------------------------|--------------|
| 1    | 127.0.0.1:5001 | 127.0.0.1:5002              | `config.txt` |
| 2    | 127.0.0.1:5002 | 127.0.0.1:5003              | `config2.txt` |
| 3    | 127.0.0.1:5003 | 127.0.0.1:5001              | `config3.txt` |

Started in three terminals:

```bash
python3 myleprocess.py config.txt  log1.txt
python3 myleprocess.py config2.txt log2.txt
python3 myleprocess.py config3.txt log3.txt
```

### Execution example (actual terminal output)

**Terminal 1 (node 1, `config.txt` / `log1.txt`):**

```
My id: d673665e-ca05-4b77-a53c-8803ae64389e
[server] listening on 127.0.0.1:5001
[client] connected to 127.0.0.1:5002
[server] accepted connection from ('127.0.0.1', 64317)
Leader is decided to d673665e-ca05-4b77-a53c-8803ae64389e.
leader is d673665e-ca05-4b77-a53c-8803ae64389e
```

**Terminal 2 (node 2, `config2.txt` / `log2.txt`):**

```
My id: b5a7e7ae-608f-4557-a9d6-5e92710592e7
[server] listening on 127.0.0.1:5002
[server] accepted connection from ('127.0.0.1', 64313)
[client] connected to 127.0.0.1:5003
Leader is decided to d673665e-ca05-4b77-a53c-8803ae64389e.
leader is d673665e-ca05-4b77-a53c-8803ae64389e
```

**Terminal 3 (node 3, `config3.txt` / `log3.txt`):**

```
My id: 7519cfae-76ab-4842-b45b-d428506bd285
[server] listening on 127.0.0.1:5003
[client] connected to 127.0.0.1:5001
[server] accepted connection from ('127.0.0.1', 64318)
Leader is decided to d673665e-ca05-4b77-a53c-8803ae64389e.
leader is d673665e-ca05-4b77-a53c-8803ae64389e
```

All three processes agree on the same `leader_id`
(`d673665e-ca05-4b77-a53c-8803ae64389e`, the largest of the three
UUIDs), and each stopped sending messages once it forwarded the
leader announcement — satisfying termination, uniqueness, and
agreement. See `log1.txt`, `log2.txt`, `log3.txt` for the full
per-message trace (received/sent, comparison result, and state).

## Classroom demo with another student

1. Agree on IP:port pairs out of band.
2. Put your own listening IP:port on line 1 of `config.txt` and your
   neighbor's IP:port (the one you'll connect to as a client) on
   line 2.
3. Run `python3 myleprocess.py config.txt log.txt --wait` so you can
   press Enter once everyone's server is up before connecting out.
