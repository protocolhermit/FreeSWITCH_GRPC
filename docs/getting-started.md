# Getting started with mod_grpc (for beginners)

This page assumes you know **nothing** about FreeSWITCH or gRPC. It explains
the idea, walks one phone call through the system, and gets a first call
working. When you are done, move on to the other docs:

| Next | Read |
|------|------|
| Every message, field and rule | [protocol.md](protocol.md) |
| Every configuration setting and console command | [user-guide.md](user-guide.md) |
| The engine in Go, how to build it for production | [building-an-engine.md](building-an-engine.md) |
| A ready-made engine snippet for each situation (voicemail, queue, conference, ...) | [engine-examples.md](engine-examples.md) |

---

## 1. The big idea

A phone system has two jobs:

1. **Move audio and SIP signalling** (the "media plane"). This is heavy,
   real-time work. **FreeSWITCH** is good at it.
2. **Make decisions** (the "control plane"): who is this caller, where should
   the call go, what should it hear, is this number allowed? This is business
   logic. It changes often and is easiest to write in a normal programming
   language with a database.

`mod_grpc` splits those jobs cleanly:

```
 phone ──SIP/audio──▶ FreeSWITCH + mod_grpc  ◀══ one gRPC stream ══▶  your engine
                       (the "node")                                    (your program)
                       moves the audio                                 makes the decisions
```

- **FreeSWITCH** is an open-source telephony server. It answers SIP calls,
  plays audio files, records, bridges two callers together, and so on.
- **mod_grpc** is a plug-in (a "module") for FreeSWITCH. It does not decide
  anything itself. At every decision point it asks your program.
- **gRPC** is a way for two programs to talk over the network using typed
  messages. Here, one connection stays open and both sides send messages on it
  whenever they like. (This is called a *bidirectional stream*.)
- **The node** is one FreeSWITCH server running `mod_grpc`. You can run many.
- **The engine** is the program *you* write. It is a gRPC **server**. Every
  node connects to it as a **client**.

Your engine never touches audio. It sends small instructions such as "play this
file", "collect one digit", "bridge to user 1001", and the node carries them
out.

---

## 2. The words you will meet

| Word | Meaning |
|------|---------|
| **Node** | One FreeSWITCH + `mod_grpc`. Identified by `node-id`. |
| **Engine** | Your gRPC server that implements `controlplane.v1.ControlPlane`. |
| **Tenant** | One customer of your platform. Every call, user and command belongs to one tenant, identified by a UUID (`tenant_id`). Their data never mixes. |
| **Domain** | The part after the `@` in a SIP address (`1001@acme.example` has domain `acme.example`). Each domain belongs to one tenant. |
| **Channel / leg** | One side of a call. A normal call has two legs: the caller and the callee. Each leg has a **UUID**. |
| **Command** | An instruction the engine sends: Bridge, Playback, Hangup, ... |
| **Batch** | A list of commands the engine returns for one call, run in order. |
| **Step** | A batch whose last command has a `step_id`. The node reports how it ended and asks for the next batch. This is how IVR menus work. |
| **Lookup** | The node asking the engine for data: "who is user 1001?", "which tenant owns acme.example?". Answers are cached. |
| **Inline command** | A command inside a `RouteResponse`. It runs on the call that asked. |
| **Stream command** | A command the engine sends on its own, at any time (click-to-call, hang up, drain). |
| **Early media** | Audio played before the call is answered. `Playback`, `Collect` and `Record` use it unless you send `Answer` first. |

---

## 3. One call, step by step

Alice (1002) dials 1001. Here is everything that happens.

```
Alice's phone      FreeSWITCH node (mod_grpc)                 Engine
     │  INVITE 1001         │                                    │
     ├─────────────────────▶│                                    │
     │                      │── LookupRequest DIRECTORY 1002 ───▶│  "who is 1002? what is her password?"
     │                      │◀──────── LookupResponse ───────────│  {"name":"Alice","a1_hash":"..."}
     │                      │  (checks her password, accepts)    │
     │                      │── RouteRequest dest=1001 ─────────▶│  "where should this call go?"
     │                      │◀── RouteResponse [Bridge 1001] ────│  "ring user/1001@acme.example"
     │   (rings Bob) ...    │── CallEvent CREATED/ANSWERED ─────▶│  (events: FYI, no reply)
     │                      │── CallEvent HANGUP ───────────────▶│
```

In words:

1. **Authenticate.** If the phone registers or sends a challenge-protected
   INVITE, the node asks the engine for the user's directory entry (a
   *lookup*). The engine answers with a name and a password hash.
2. **Route.** The dialplan runs the `grpc_route` application. It sends a
   `RouteRequest` (who is calling, what number was dialled, which domain). The
   engine answers with a *batch of commands*.
3. **Run the commands.** The node bridges, plays, collects, and so on.
4. **Report.** If the engine asked (`step_id`), the node reports the outcome and
   the engine answers with the next batch. Otherwise the node just runs the
   batch to the end.
5. **Events.** The node tells the engine when the call is created, answered,
   a key is pressed, and when it hangs up.

### An IVR ("press 1 for sales"), as a conversation

```
Node:    RouteRequest  dest=800
Engine:  RouteResponse [ Answer, Collect(prompt="welcome.wav", 1 digit) step_id="menu" ]
Node:    (answers, plays welcome.wav, caller presses 1)
Node:    StepResult    step_id="menu"  collect.digits="1"
Engine:  RouteResponse [ Bridge(user/1001@acme.example) step_id="sales" ]
Node:    (rings 1001, they answer, the call ends)
Node:    StepResult    step_id="sales" bridge.answered=true
Engine:  RouteResponse [ Hangup ]
```

The engine keeps **no memory per call**. The state lives in the `step_id` and in
the result the node sends back. That is why retries are safe and why any copy
of the engine can answer any node.

---

## 4. The five rules that make everything work

1. **The first message on a stream is `Hello`.** Check its `token`, reply
   `HelloAck`.
2. **Echo the `request_id`.** Every reply must carry the `request_id` of the
   request it answers. Without it the node never sees your answer.
3. **Be quick.** Route and step answers: under 3 s. Directory lookups: under
   1.5 s. Slower replies are dropped.
4. **Always set `tenant_id` on a command.** The node checks that this tenant
   owns the call and refuses the command if not.
5. **Say `unavailable=true` when your database is down.** That is different
   from "not found", and it makes the node serve stale data or fail the call
   cleanly.

---

## 5. Your first call in about ten minutes

You need: a FreeSWITCH install (see [user-guide.md](user-guide.md) §1 to build
the module), Go 1.21+, and `protoc`.

**Step 1. Write the engine.** Copy the engine from
[building-an-engine.md](building-an-engine.md) §3 and run it. It listens on
`:9443`, accepts the token `replace-with-a-real-secret`, and routes
`1001`/`1002` to each other.

```bash
go run .
```

**Step 2. Configure the node.** Create
`autoload_configs/mod_grpc.conf.xml`. For a lab, plaintext is fine; never do
this in production.

```xml
<configuration name="mod_grpc.conf">
  <settings>
    <param name="engine-endpoints" value="127.0.0.1:9443"/>
    <param name="node-id"          value="fs-01"/>
    <param name="password"         value="replace-with-a-real-secret"/>
  </settings>
</configuration>
```

Add `<load module="mod_grpc"/>` to `modules.conf.xml`, or run `load mod_grpc`
in `fs_cli`.

**Step 3. Check the connection.**

```
freeswitch> grpc status
node=fs-01 endpoints=127.0.0.1:9443 connected=yes ...
```

`connected=yes` means the `Hello`/`HelloAck` handshake worked. If it says `no`,
see section 7.

**Step 4. Send calls to the engine.** In your dialplan:

```xml
<extension name="engine">
  <condition field="destination_number" expression="^.*$">
    <action application="grpc_route"/>
  </condition>
</extension>
```

**Step 5. Make a test call without any phone.** In `fs_cli`:

```
originate {tenant_id=6f1c2a8e-3b7d-4c11-9a0e-2d5f6b7c8d90,domain_name=acme.example}null/1001 &grpc_route()
```

Watch the engine log: you should see the `RouteRequest` arrive and the
`CallEvent`s follow.

---

## 6. Which "mode" am I in?

`registration-mode` decides who holds the phones' registrations.

| Mode | Who holds registrations | Who names the tenant | Pick it when |
|------|------------------------|----------------------|--------------|
| `edge` (default) | A SIP proxy in front, such as Kamailio | The proxy, in the `X-Tenant-Id` header | You already have a SIP edge |
| `local` | This FreeSWITCH node | The domain, via a map in the config or an engine lookup | One node, no proxy |
| `engine` | This node, and it reports each registration to the engine | Same as `local` | Several nodes, no proxy: the engine knows which node holds each phone |

Details are in [user-guide.md](user-guide.md) §9 and
[building-an-engine.md](building-an-engine.md) §8.

---

## 7. When something does not work

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| `grpc status` shows `connected=no` | Wrong `engine-endpoints`, engine not running, TLS mismatch | Check the endpoint, the engine log, and `tls-ca` / `tls-server-name`. Without `tls-ca` the node speaks plaintext, so the engine must too. |
| FreeSWITCH log: `hello refused: bad token` | The engine rejected the token | Make the engine's expected token match `password` / `token-file` |
| Module does not load | A required setting is missing, or the password is still `change-me` | Read the FreeSWITCH log: it names the reason (list in [user-guide.md](user-guide.md) §2) |
| Call hangs up at once, log says `no tenant for domain ...` | The engine did not answer the `domain:<fqdn>` lookup | Return `{"tenant_id":"<uuid>"}` for that `CONFIG` key (see [engine-examples.md](engine-examples.md) §2) |
| Call hangs up with `NORMAL_TEMPORARY_FAILURE` | Route timed out (over 3 s), engine returned `unavailable`, or an empty batch | Log what your `route` handler returns; make it fast |
| A command comes back with an `error` | The node validated it and refused (bad dial string, wrong tenant, path with `..`) | Read `StepResult.error` / `CommandResult.error`; rules are in [building-an-engine.md](building-an-engine.md) §7 |
| A change in the engine is not visible on the node | The node is serving its cache | Send an `Invalidate`, or run `grpc cache flush` while testing |
| Audio missing in the first seconds of a long prompt | The call is not answered yet | Send `Answer` first |

Handy console commands while debugging: `grpc status`, `grpc tenant <domain>`,
`grpc lookup <tenant> directory 1001@acme.example`, `grpc cache flush`.
