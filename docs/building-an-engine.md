# Building an engine for mod_grpc

The **engine** is a gRPC server that implements `controlplane.v1.ControlPlane`.
Every FreeSWITCH node running `mod_grpc` connects to it and asks it how to:

- authenticate users
- route calls
- run IVR steps
- serve configuration

You can write it in any gRPC language. This guide uses Go, which is what the
proto's `go_package` option targets.

Read [protocol.md](protocol.md) first. This guide shows how the pieces fit
together in code.

---

## 1. What an engine must do

On each `Connect` stream:

1. **Read `Hello`** and authenticate `token`. Reply `HelloAck{accepted}`.
2. **Loop on `Recv()`** and dispatch on the message body:
   - `LookupRequest` → `LookupResponse`, echoing `request_id`
   - `RouteRequest` → `RouteResponse`, echoing `request_id`
   - `StepResult` → `RouteResponse` with the next batch, echoing `request_id`
   - `Heartbeat` / `CallEvent` / `ConfEvent` / `RegEvent` → record them, no reply
   - `CommandResult` → match it to a `Command` you sent, by `command_id`
3. **Push** `Command`, `Invalidate`, and `TokenRotate` whenever you need to.
   Keep a registry `node_id → stream` so other parts of your system can reach
   a node.

Rules that matter:

| Rule | Why |
|------|-----|
| Echo `request_id` on every reply | The node matches replies by it. A missing or wrong id means the node times out. |
| Answer routes in < 3 s and directory lookups in < 1.5 s | Past that, the node gives up: it rejects the call or serves stale data. |
| Serialize `stream.Send` | gRPC streams are not safe for concurrent `Send`. Use one mutex or one writer goroutine per stream. |
| Don't block the `Recv` loop on slow work | Handle each request in a goroutine. The stream carries every call on the node. |
| Set `tenant_id` on every `Command` | The node refuses commands whose tenant doesn't own the channel. |
| Make `StepResult` handling idempotent | The node retries the same `step_id` up to 6 times. |
| Send `unavailable=true` on backend failure | It tells the node "temporary failure", which differs from "not found" or "no route". |
| Send `Invalidate` when data changes | Otherwise the node serves its cached copy until `ttl_s` runs out. |

---

## 2. Generate the stubs

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

mkdir engine && cd engine
go mod init example.com/engine
protoc -I ../proto \
  --go_out=gen --go_opt=paths=source_relative \
  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
  controlplane/v1/control.proto
go get google.golang.org/grpc google.golang.org/protobuf
```

This writes `gen/controlplane/v1/control.pb.go` and `control_grpc.pb.go`,
package `controlplanev1`.

For other languages, run the same `protoc` command with that language's
plugin, for example `grpc_tools.protoc` for Python or `grpc-java`.

---

## 3. A minimal engine

This engine:

- accepts nodes that present a known token
- serves directory entries for two users
- maps a domain to a tenant
- routes calls with a small IVR: "press 1 for sales"

```go
package main

import (
	"log"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"

	pb "example.com/engine/gen/controlplane/v1"
)

const (
	nodeToken = "replace-with-a-real-secret"
	tenant    = "6f1c2a8e-3b7d-4c11-9a0e-2d5f6b7c8d90" // tenant UUID
	domain    = "acme.example"
)

var users = map[string]string{"1001@" + domain: "Alice", "1002@" + domain: "Bob"}

type engine struct {
	pb.UnimplementedControlPlaneServer
	nodes sync.Map // node_id -> *node, so the rest of the system can push commands
}

type node struct {
	id     string
	mu     sync.Mutex // serializes Send
	stream pb.ControlPlane_ConnectServer
}

func (n *node) send(m *pb.EngineMessage) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stream.Send(m); err != nil {
		log.Printf("node %s: send: %v", n.id, err)
	}
}

func (e *engine) Connect(stream pb.ControlPlane_ConnectServer) error {
	// 1. Handshake: the first message must be Hello.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := first.GetHello()
	if h == nil || h.Token != nodeToken {
		return stream.Send(&pb.EngineMessage{RequestId: first.RequestId,
			Body: &pb.EngineMessage_HelloAck{HelloAck: &pb.HelloAck{Accepted: false, Reason: "bad token"}}})
	}
	n := &node{id: h.NodeId, stream: stream}
	n.send(&pb.EngineMessage{RequestId: first.RequestId, Body: &pb.EngineMessage_HelloAck{HelloAck: &pb.HelloAck{
		Accepted: true, SessionId: h.NodeId + "/" + h.BootId, HeartbeatIntervalS: 5}}})
	e.nodes.Store(h.NodeId, n)
	defer e.nodes.Delete(h.NodeId)
	log.Printf("node %s connected (version %s, capacity %d, tags %v)", h.NodeId, h.Version, h.Capacity, h.Tags)

	// 2. Dispatch loop. Each request runs in a goroutine so a slow one never stalls the stream.
	for {
		m, err := stream.Recv()
		if err != nil {
			log.Printf("node %s gone: %v", h.NodeId, err)
			return err
		}
		id := m.RequestId
		switch b := m.Body.(type) {
		case *pb.NodeMessage_Lookup:
			go n.send(&pb.EngineMessage{RequestId: id, Body: &pb.EngineMessage_LookupResponse{LookupResponse: lookup(b.Lookup)}})
		case *pb.NodeMessage_RouteRequest:
			go n.send(&pb.EngineMessage{RequestId: id, Body: &pb.EngineMessage_RouteResponse{RouteResponse: route(b.RouteRequest)}})
		case *pb.NodeMessage_StepResult:
			go n.send(&pb.EngineMessage{RequestId: id, Body: &pb.EngineMessage_RouteResponse{RouteResponse: step(b.StepResult)}})
		case *pb.NodeMessage_CallEvent:
			ev := b.CallEvent
			log.Printf("call %s %s digit=%q cause=%q", ev.Uuid, ev.Type, ev.Digit, ev.Cause)
		case *pb.NodeMessage_CommandResult:
			log.Printf("command %s ok=%v %s", b.CommandResult.CommandId, b.CommandResult.Ok, b.CommandResult.Error)
		case *pb.NodeMessage_Heartbeat, *pb.NodeMessage_ConfEvent:
			// store for load balancing / dashboards
		}
	}
}

func lookup(r *pb.LookupRequest) *pb.LookupResponse {
	switch {
	case r.Kind == pb.LookupKind_LOOKUP_KIND_CONFIG && r.TenantId == "" && r.Key == "domain:"+domain:
		return &pb.LookupResponse{Found: true, TtlS: 300, Payload: []byte(`{"tenant_id":"` + tenant + `"}`)}
	case r.Kind == pb.LookupKind_LOOKUP_KIND_DIRECTORY && r.TenantId == tenant:
		if name, ok := users[r.Key]; ok {
			return &pb.LookupResponse{Found: true, Version: 1, TtlS: 60, Payload: []byte(`{"name":"` + name + `"}`)}
		}
	}
	return &pb.LookupResponse{Found: false, TtlS: 30}
}

// route: the first batch for a new call.
func route(r *pb.RouteRequest) *pb.RouteResponse {
	if r.TenantId != tenant {
		return &pb.RouteResponse{Commands: []*pb.Command{hangup(r.TenantId, r.Uuid, "CALL_REJECTED")}}
	}
	if strings.HasPrefix(r.DestinationNumber, "10") { // extension-to-extension: bridge straight away
		return &pb.RouteResponse{Commands: []*pb.Command{{TenantId: tenant, Action: &pb.Command_Bridge{Bridge: &pb.Bridge{
			Uuid: r.Uuid, DialString: "user/" + r.DestinationNumber + "@" + domain, TimeoutS: 30}}}}}
	}
	// IVR: play a menu, read one digit, and report back (step_id on the last command).
	return &pb.RouteResponse{Commands: []*pb.Command{{
		TenantId: tenant, StepId: "menu",
		Action: &pb.Command_Collect{Collect: &pb.Collect{Uuid: r.Uuid, Prompt: "ivr/ivr-welcome.wav",
			MinDigits: 1, MaxDigits: 1, TimeoutMs: 5000, Terminators: "#"}},
	}}}
}

// step: the next batch, decided from how the last one ended. Same input, same answer (the node may retry).
func step(s *pb.StepResult) *pb.RouteResponse {
	if s.HungUp {
		return &pb.RouteResponse{} // ignored by the node
	}
	switch s.StepId {
	case "menu":
		if c := s.GetCollect(); c != nil && c.Digits == "1" {
			return &pb.RouteResponse{Commands: []*pb.Command{{TenantId: tenant, StepId: "sales",
				Action: &pb.Command_Bridge{Bridge: &pb.Bridge{Uuid: s.Uuid, DialString: "user/1001@" + domain, TimeoutS: 25}}}}}
		}
	case "sales":
		if b := s.GetBridge(); b != nil && b.Answered {
			return &pb.RouteResponse{Commands: []*pb.Command{hangup(tenant, s.Uuid, "NORMAL_CLEARING")}}
		}
	}
	return &pb.RouteResponse{Commands: []*pb.Command{
		{TenantId: tenant, Action: &pb.Command_Playback{Playback: &pb.Playback{Uuid: s.Uuid, File: "ivr/ivr-call_cannot_be_completed_as_dialed.wav"}}},
		hangup(tenant, s.Uuid, "NORMAL_CLEARING"),
	}}
}

func hangup(t, uuid, cause string) *pb.Command {
	return &pb.Command{TenantId: t, Action: &pb.Command_Hangup{Hangup: &pb.Hangup{Uuid: uuid, Cause: cause}}}
}

func main() {
	lis, err := net.Listen("tcp", ":9443")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer() // production: grpc.Creds(credentials.NewTLS(...)) with client-cert verification
	pb.RegisterControlPlaneServer(s, &engine{})
	log.Println("engine listening on :9443")
	log.Fatal(s.Serve(lis))
}
```

To run it, point a node at the engine:

```xml
<param name="engine-endpoints" value="engine-host:9443"/>
<param name="node-id" value="fs-01"/>
<param name="password" value="replace-with-a-real-secret"/>
```

Then add `grpc_route` to the dialplan (see [user-guide.md](user-guide.md) §3),
and check the connection with `grpc status`.

---

## 4. Pushing commands (outside a call's flow)

Engine logic outside a `RouteRequest` sends commands through the node's
stream:

- click-to-call
- supervisor barge
- a hangup button
- draining a node for maintenance

```go
func (e *engine) push(nodeID string, c *pb.Command) bool {
	v, ok := e.nodes.Load(nodeID)
	if !ok {
		return false
	}
	v.(*node).send(&pb.EngineMessage{Body: &pb.EngineMessage_Command{Command: c}})
	return true
}

// click-to-call: ring 1001 and, when it answers, hand the leg back to route() as a new call
e.push("fs-01", &pb.Command{CommandId: "c-42", TenantId: tenant,
	Action: &pb.Command_Originate{Originate: &pb.Originate{DialString: "user/1001@" + domain, TimeoutS: 30,
		Vars: map[string]string{"origination_caller_id_number": "1001"}}}})
```

The node answers with `CommandResult{command_id:"c-42"}`. You'll see it in
the `Recv` loop.

The node refuses the blocking actions (`Collect`, `Record`, `Conference`,
`Sleep`, `Amd`) on this path. Use them only inline in a
`RouteResponse`.

To find the node a call is on, track it from `RouteRequest` and `CallEvent`
(`uuid` → `node_id`).

---

## 5. Caching and invalidation

The node caches every `LookupResponse` for `ttl_s` seconds. When data
changes, for example a user is renamed or deleted, tell every connected node:

```go
inv := &pb.EngineMessage{Body: &pb.EngineMessage_Invalidate{Invalidate: &pb.Invalidate{
	TenantId: tenant, Kind: pb.LookupKind_LOOKUP_KIND_DIRECTORY, Key: "1001@" + domain, Version: 2}}}
e.nodes.Range(func(_, v any) bool { v.(*node).send(inv); return true })
```

- `Kind: 0` matches every kind. `Key: "*"` matches every key in the tenant.
- After a reconnect the node expires its whole cache itself, so invalidations
  missed while it was disconnected don't matter.

---

## 6. Production checklist

- **TLS / mTLS:**
  - Serve with a certificate whose name matches the nodes' `tls-server-name`.
  - Require client certs signed by the CA you give nodes, and set
    `require-tls=true` on the nodes.
- **Authentication:**
  - Give each node its own token, stored hashed.
  - Rotate tokens with `TokenRotate{token, overlap_s}` and accept the old one
    for `overlap_s`.
  - Use `token-file` on the node so the rotated token survives restarts.
- **High availability:**
  - List several engines in `engine-endpoints`. The node tries them
    round-robin with backoff from 0.5 s to 15 s.
  - Every engine instance must be able to answer for any node: shared state
    such as Redis or a database, not local memory.
- **Restarts:** a changed `Hello.boot_id` means FreeSWITCH restarted. Clear
  any calls you had recorded for that node.
- **Capacity and load:** use `Hello.capacity` and `Heartbeat.active_calls` /
  `cpu_pct` to choose nodes for `Originate`.
- **Draining:**
  - Push `Command{Drain{deadline_s}}` before maintenance. New inbound calls
    stop and existing calls finish.
  - On the node, `grpc undrain` reverses it.
- **Survival:**
  - If nodes run `survival=true`, serve `CONFIG` key `survival` per tenant.
    The snapshot shape is in [protocol.md](protocol.md) §3.
  - The snapshot is all a node has while your engine is down.
- **Backend failures:** reply `unavailable=true` (`LookupResponse` /
  `RouteResponse`) instead of `found=false` or an empty batch. The node then
  keeps serving stale cache data and fails calls cleanly.

---

## 7. Dial strings, prompts, and other values the node validates

The node rejects a command with an `error` (visible in `StepResult.error` or
`CommandResult.error`) when any of these rules fails.

**Dial strings**

- No whitespace.
- No `${...}` or other variable expansion.
- At most one leading `[var=value,...]` block per leg.
- Legs are joined by `,` (ring together) or `|` (one after another).
- The node adds `X-Tenant-Id` itself.

**Prompts and files**

- No `..`, and no `//` except after a URL scheme.
- The only allowed URL schemes are `silence_stream://` and `tone_stream://`.
- Files under `record-dir` must be inside the channel's own tenant directory.

**Collect**

- `max_digits` must be 1..32.
- `timeout_ms` must be 1..60000.
- `terminators` must be a subset of `0123456789#*`.

**Record names, rooms, and profiles**

- Each must be a single safe path component.

**Transfer**

- `token` must be UUID-shaped.

---

## 8. Registrations and multi-node routing

This section applies to nodes running `registration-mode=engine` (see
[user-guide.md](user-guide.md) §9). The phones register on the nodes, and the
engine learns about it from `RegEvent`s. It then decides which node to ring.
Nodes in `edge` or `local` mode send no `RegEvent`.

### What the engine receives

- `Hello.sip_address`: the `host:port` other nodes dial. Keep it per `node_id`.
- `Hello.registration_mode`: `"engine"`.
- `RegEvent` with `type` `REGISTER`, `UNREGISTER`, or `EXPIRE`, one per
  change. The node also replays its live registrations after every reconnect,
  with `resync=true` (see [protocol.md](protocol.md) §6).

### Serve the `Registrations` RPC

Implement `Registrations`, so registration traffic stays off the `Connect`
stream that carries calls. Without it, everything still works: the node falls
back to sending `RegEvent` on `Connect`. But a replay of thousands of
registrations then shares a queue with `RouteRequest`, and a slow
registration handler delays calls.

- Read the first message: it must be `open`. Check `token`, and check that
  `session_id` is the live session of `node_id`. Answer
  `RegStreamAck{accepted: true}`, or `false` with a `reason`.
- Then read `event` messages until the stream ends. Give each stream its own
  goroutine. Events for one user are always on the same stream, in order.
  Don't reorder within a stream, for example by fanning one stream's events
  out to a worker pool.
- Close the node's registration streams when its `Connect` stream ends. The
  node does the same, and it treats a closed registration stream as a lost
  session: it reconnects and replays.
- Pick the count on the node with `registration-streams`. 1 is enough for most
  nodes. Raise it to 4–8 for nodes with tens of thousands of registrations,
  when one goroutine storing to Redis can't keep up.

```go
func (e *engine) Registrations(st pb.ControlPlane_RegistrationsServer) error {
	m, err := st.Recv()
	if err != nil || m.GetOpen() == nil {
		return status.Error(codes.InvalidArgument, "open first")
	}
	n := e.liveNode(m.GetOpen().NodeId, m.GetOpen().SessionId)
	if n == nil || !e.tokenOK(n, m.GetOpen().Token) {
		_ = st.Send(&pb.RegStreamAck{Accepted: false, Reason: "unknown session"})
		return nil
	}
	if err := st.Send(&pb.RegStreamAck{Accepted: true}); err != nil {
		return err
	}
	for {
		m, err := st.Recv()
		if err != nil {
			return nil
		}
		if r := m.GetEvent(); r != nil {
			e.onReg(st.Context(), n, r) // in order: no goroutine per event
		}
	}
}
```

### Store: one Redis hash per user

Key `reg:{tenant}:{user}@{domain}`. One field per `call_id` (one per device),
holding JSON:

```json
{"node_id":"fs-01","sip_address":"10.0.0.11:5080","contact":"<sip:1001@10.1.2.3:5060>","expires_at":1763900000}
```

- `REGISTER`: `HSET` the field, then `HEXPIRE` that field for `expires_s`
  seconds. A device that vanishes disappears on its own.
- `UNREGISTER` and `EXPIRE`: `HDEL` the field.
- Treat `expires_s` as a TTL, not an exact time. A replayed value can run up to
  about a minute longer than the phone's `Expires`.
- Per-field `HEXPIRE` needs **Redis 7.4 or newer**. On older Redis, use one key
  per device, `reg:{tenant}:{user}@{domain}:{call_id}`, with `SET ... EX`, plus a
  set `regs:{tenant}:{user}@{domain}` of those keys. Read the set, then fetch
  each key and drop members whose key is gone.

### Two rules for replays

- **A live `UNREGISTER` wins.** A replay is a snapshot, and it can add a
  device that unregistered while it ran. Remember each unregistered `call_id`
  for a couple of minutes, and ignore a `resync` `REGISTER` for it.
- **A changed `Hello.boot_id` means that node restarted.** Delete every
  registration you hold for its `node_id` before accepting the new stream's
  events. The node's replay then fills them in again. Do the same when a node
  is gone for good.

### Routing a call to a registered user

On `RouteRequest` for `user@domain`, read the hash and group the devices by
`node_id`. The node that sent the request is the **local** one.

| Case | Answer |
|------|--------|
| Device on this node only | `Bridge{DialString: "user/<user>@<domain>"}` |
| Devices on other nodes | `Bridge{PeerAddresses: <sip_address of each>, PeerTarget: "<user>@<domain>"}` and a `DialString` for local devices (empty if none) |
| `vars["hop"] == "1"` | The call already hopped from another node. Use **local devices only**, with no `PeerAddresses`. The node refuses a second hop. |

Trust only the `hop` var for this. The node sets it only when the hop came from
a source that passed `tenant-header-acl`. The request may also carry the
caller's headers as `sip_h_X-*` vars, including `sip_h_X-Grpc-Hop`, which any
phone can send. Never treat that header var as a hop.

### Ring strategies

Chosen by the engine, per tenant, with an optional per-user override. The node
doesn't know about them.

| Strategy | Rings |
|----------|-------|
| `all` | Every device on every node, together. |
| `last-registered` | The device with the latest registration only. |
| `local-first` | Devices on this node. Peers only if none is local. |

### A `RegEvent` handler in Go

Uses `github.com/redis/go-redis/v9` (`HSet`, `HExpire`, `HDel`, `HGetAll`;
`HExpire` needs v9.6 or newer and Redis 7.4). It is illustrative and was not
compiled or run against a live Redis. The `unreg:<call_id>` tombstone and the
`node:<id>` set are a suggestion beyond the spec: one way to let a live
`UNREGISTER` win over a late replay, and to drop a restarted node's devices.

```go
import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	pb "example.com/engine/gen/controlplane/v1"
)

type regInfo struct {
	NodeID     string `json:"node_id"`
	SipAddress string `json:"sip_address"`
	Contact    string `json:"contact"`
	ExpiresAt  int64  `json:"expires_at"`
}

func regKey(r *pb.RegEvent) string { return "reg:" + r.TenantId + ":" + r.User + "@" + r.Domain }

// From Registrations (above), or on Connect for a node that fell back:
// case *pb.NodeMessage_RegEvent: e.onReg(ctx, n, b.RegEvent)
func (e *engine) onReg(ctx context.Context, n *node, r *pb.RegEvent) {
	key := regKey(r)
	switch r.Type {
	case pb.RegEvent_REGISTER:
		if r.Resync && e.rdb.Exists(ctx, "unreg:"+r.CallId).Val() > 0 {
			return // a live UNREGISTER already won
		}
		v, _ := json.Marshal(regInfo{n.id, n.sipAddress, r.Contact, time.Now().Unix() + int64(r.ExpiresS)})
		e.rdb.HSet(ctx, key, r.CallId, v)
		e.rdb.HExpire(ctx, key, time.Duration(r.ExpiresS)*time.Second, r.CallId)
		e.rdb.SAdd(ctx, "node:"+n.id, key+"|"+r.CallId) // so a restart can drop them
	case pb.RegEvent_UNREGISTER, pb.RegEvent_EXPIRE:
		e.rdb.HDel(ctx, key, r.CallId)
		e.rdb.SRem(ctx, "node:"+n.id, key+"|"+r.CallId)
		if r.Type == pb.RegEvent_UNREGISTER && !r.Resync {
			e.rdb.Set(ctx, "unreg:"+r.CallId, 1, 2*time.Minute)
		}
	}
}

// Call when a node's Hello carries a different boot_id than the last one seen for it.
func (e *engine) dropNode(ctx context.Context, nodeID string) {
	for _, m := range e.rdb.SMembers(ctx, "node:"+nodeID).Val() {
		i := strings.LastIndex(m, "|")
		e.rdb.HDel(ctx, m[:i], m[i+1:])
	}
	e.rdb.Del(ctx, "node:"+nodeID)
}

// Devices of a user, for route(): grouped by node by the caller.
func (e *engine) devices(ctx context.Context, tenant, user, domain string) []regInfo {
	var out []regInfo
	for _, v := range e.rdb.HGetAll(ctx, "reg:"+tenant+":"+user+"@"+domain).Val() {
		var ri regInfo
		if json.Unmarshal([]byte(v), &ri) == nil {
			out = append(out, ri)
		}
	}
	return out
}
```

The snippet needs `rdb *redis.Client` on `engine`, `sipAddress string` on
`node` (set from `h.SipAddress` in `Connect`). Call `dropNode` from `Connect`
when `h.BootId` differs from the one last stored for that node. The `node:<id>`
set can hold stale members after an `HEXPIRE` expiry. `HDel` on a missing field
does nothing, so that is harmless.
