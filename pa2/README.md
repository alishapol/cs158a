# PA2 - Leader Election

This is my implementation of the Chang-Roberts leader election algorithm
for an asynchronous ring, using UUIDs as process ids.

## Files

- myleprocess.py - the actual process. handles connecting to neighbors,
  sending/receiving messages, and the election logic
- config.txt - my config for the demo (line 1 = my server ip/port,
  line 2 = the neighbor I connect out to)
- config2.txt, config3.txt - configs for the other two instances I used
  to test locally
- log1.txt, log2.txt, log3.txt - logs from running the three instances

## How it works

Every node has two connections:

- a server socket (bind/listen/accept) - whoever connects to this is
  the neighbor we receive messages from
- a client socket (connect) - the neighbor we send messages to

So the direction messages travel around the ring depends on who connects
to who's server, based on the config files.

accept() has to run in its own thread. If it didn't, every node would
call accept() and just sit there forever, since nobody would ever get
around to calling connect(). Once both connections are actually up
there's no more need for extra threads, the rest is just read a
message, decide what to do with it, maybe send one out, repeat.

Each Message is sent as JSON. Instead of using a newline to mark where
one message ends and the next starts, I read bytes off the socket and
just watch for the closing "}" - once I see that, everything up to and
including it is one full message and whatever's left over gets kept
for the next one.

election logic (based on Chang-Roberts):
- send my own id out first, no comparison
- when I get a message with flag 0:
  - if the id is bigger than mine, forward it
  - if it's smaller, drop it
  - if it's equal to mine, that means it went all the way around back
    to me, so I'm the leader
- once someone becomes the leader they send flag 1 around so everyone
  else finds out, and each node only forwards that once before stopping

## how to run it

one process:

```
python3 myleprocess.py config.txt log.txt
```

you can also pass --wait if you want it to pause before connecting out
(useful so everyone's server is up first):

```
python3 myleprocess.py config.txt log.txt --wait
```

if you don't pass any args it defaults to config.txt and log.txt

## running the 3 process demo locally

ran 3 copies on localhost, different ports, ring goes node1 -> node2 ->
node3 -> node1:

- node1: server on 5001, connects to 5002 (config.txt)
- node2: server on 5002, connects to 5003 (config2.txt)
- node3: server on 5003, connects to 5001 (config3.txt)

opened 3 terminals and ran:

```
python3 myleprocess.py config.txt log1.txt
python3 myleprocess.py config2.txt log2.txt
python3 myleprocess.py config3.txt log3.txt
```

### output from the terminals

terminal 1:
```
My id: 983c75f6-8d9c-4d36-8c7a-51c519c227ce
[server] listening on 127.0.0.1:5001
[client] connected to 127.0.0.1:5002
[server] accepted connection from ('127.0.0.1', 50803)
Leader is decided to 983c75f6-8d9c-4d36-8c7a-51c519c227ce.
leader is 983c75f6-8d9c-4d36-8c7a-51c519c227ce
```

terminal 2:
```
My id: 12b77179-630d-40f3-93f0-55c16334faa8
[server] listening on 127.0.0.1:5002
[server] accepted connection from ('127.0.0.1', 50797)
[client] connected to 127.0.0.1:5003
Leader is decided to 983c75f6-8d9c-4d36-8c7a-51c519c227ce.
leader is 983c75f6-8d9c-4d36-8c7a-51c519c227ce
```

terminal 3:
```
My id: 746f14f0-062b-4903-96b7-53e52d46afd3
[server] listening on 127.0.0.1:5003
[client] connected to 127.0.0.1:5001
[server] accepted connection from ('127.0.0.1', 50804)
Leader is decided to 983c75f6-8d9c-4d36-8c7a-51c519c227ce.
leader is 983c75f6-8d9c-4d36-8c7a-51c519c227ce
```

all three agree on the same leader (983c75f6..., which is the biggest
of the 3 ids), and they all stop on their own after forwarding the
leader announcement once. check log1/log2/log3.txt for the full
message-by-message trace.

## running with another student in class

1. figure out ip/port with your partner ahead of time
2. put your own ip/port on line 1 of config.txt, theirs on line 2
3. run with --wait so you both have time to get your server up before
   connecting out
