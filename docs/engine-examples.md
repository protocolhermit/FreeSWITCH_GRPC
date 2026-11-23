# Engine cookbook: one example per situation

Each section is a self-contained piece of an engine for one situation. They all
plug into the minimal engine in [building-an-engine.md](building-an-engine.md)
§3 (the `Connect` loop, `node.send`, `route`, `step`). New to all this? Read
[getting-started.md](getting-started.md) first.

All snippets are illustrative Go. They were checked against
`proto/controlplane/v1/control.proto` but not run against a live node.

Contents:

1. [Shared helpers](#1-shared-helpers)
2. [Handshake and capabilities](#2-handshake-and-capabilities)
3. [Lookups: domain, directory, XML, survival](#3-lookups)
4. [Routing: the first batch](#4-routing-the-first-batch)
5. [Multi-step calls](#5-multi-step-calls)
6. [Stream commands](#6-stream-commands)
7. [Events](#7-events)
8. [Operations: invalidate, drain, token rotation, failures](#8-operations)
9. [Testing an engine without phones](#9-testing)

---

## 1. Shared helpers

Small constructors keep the cases below short. Every command needs the tenant
and (inline) the call `uuid`.

```go
package main

import (
	pb "example.com/engine/gen/controlplane/v1"
)

func cmd(tenant string, a any) *pb.Command {
	c := &pb.Command{TenantId: tenant}
	switch v := a.(type) {
	case *pb.Answer:
		c.Action = &pb.Command_Answer{Answer: v}
	case *pb.Bridge:
		c.Action = &pb.Command_Bridge{Bridge: v}
	case *pb.Park:
		c.Action = &pb.Command_Park{Park: v}
	case *pb.Playback:
		c.Action = &pb.Command_Playback{Playback: v}
	case *pb.Hangup:
		c.Action = &pb.Command_Hangup{Hangup: v}
	case *pb.Collect:
		c.Action = &pb.Command_Collect{Collect: v}
	case *pb.Record:
		c.Action = &pb.Command_Record{Record: v}
	case *pb.Conference:
		c.Action = &pb.Command_Conference{Conference: v}
	case *pb.Sleep:
		c.Action = &pb.Command_Sleep{Sleep: v}
	case *pb.Amd:
		c.Action = &pb.Command_Amd{Amd: v}
	case *pb.Transfer:
		c.Action = &pb.Command_Transfer{Transfer: v}
	case *pb.Originate:
		c.Action = &pb.Command_Originate{Originate: v}
	case *pb.Eavesdrop:
		c.Action = &pb.Command_Eavesdrop{Eavesdrop: v}
	case *pb.ConferenceControl:
		c.Action = &pb.Command_ConferenceControl{ConferenceControl: v}
	case *pb.MessageWaiting:
		c.Action = &pb.Command_MessageWaiting{MessageWaiting: v}
	case *pb.Drain:
		c.Action = &pb.Command_Drain{Drain: v}
	default:
		panic("unknown action")
	}
	return c
}

// withStep marks the LAST command of a batch: the node will report how it ended.
func withStep(c *pb.Command, id string) *pb.Command { c.StepId = id; return c }

func batch(cs ...*pb.Command) *pb.RouteResponse { return &pb.RouteResponse{Commands: cs} }

func hangup(t, uuid, cause string) *pb.Command {
	return cmd(t, &pb.Hangup{Uuid: uuid, Cause: cause})
}
```

---

## 2. Handshake and capabilities

**Situation:** a node connects. You authenticate it and say which optional
features your engine serves.

```go
func (e *engine) hello(h *pb.Hello, reqID string) *pb.EngineMessage {
	if !e.tokenOK(h.NodeId, h.Token) { // compare hashed, constant-time
		return &pb.EngineMessage{RequestId: reqID, Body: &pb.EngineMessage_HelloAck{
			HelloAck: &pb.HelloAck{Accepted: false, Reason: "bad token"}}}
	}
	// A new boot_id for a known node means FreeSWITCH restarted: forget its calls.
	if old, ok := e.bootIDs.Load(h.NodeId); ok && h.BootId != "" && old != h.BootId {
		e.forgetCalls(h.NodeId)
	}
	e.bootIDs.Store(h.NodeId, h.BootId)

	return &pb.EngineMessage{RequestId: reqID, Body: &pb.EngineMessage_HelloAck{HelloAck: &pb.HelloAck{
		Accepted:           true,
		SessionId:          h.NodeId + "/" + h.BootId,
		HeartbeatIntervalS: 5,
		// "data": answers DataRequest. "media_url": sends Playback.url. "mwi": sends MessageWaiting.
		Features: []string{"data", "media_url", "mwi"},
	}}}
}

// Before using Answer, check the node supports it:
func supportsAnswer(h *pb.Hello) bool {
	for _, f := range h.Features {
		if f == "answer" {
			return true
		}
	}
	return false
}
```

Keep `h.Features`, `h.Tags` (includes `api-batch`), `h.SipAddress` and
`h.RegistrationMode` on your `node` struct. You will want them later.

---

## 3. Lookups

The node asks, you answer with `LookupResponse` and the same `request_id`. One
`lookup` function handles every kind:

```go
func (e *engine) lookup(r *pb.LookupRequest) *pb.LookupResponse {
	ctx := context.Background() // use a short timeout in real code
	switch {
	case r.Kind == pb.LookupKind_LOOKUP_KIND_CONFIG && r.TenantId == "" && strings.HasPrefix(r.Key, "domain:"):
		return e.domainLookup(ctx, strings.TrimPrefix(r.Key, "domain:"))
	case r.Kind == pb.LookupKind_LOOKUP_KIND_DIRECTORY:
		return e.directoryLookup(ctx, r.TenantId, r.Key)
	case r.Kind == pb.LookupKind_LOOKUP_KIND_CONFIG && r.Key == "survival":
		return e.survivalLookup(ctx, r.TenantId)
	case r.Kind == pb.LookupKind_LOOKUP_KIND_XML:
		return e.xmlLookup(ctx, r)
	}
	return &pb.LookupResponse{Found: false, TtlS: 30}
}
```

### 3.1 Domain to tenant

**Situation:** a call arrives for `acme.example` with no `X-Tenant-Id`
(`local`/`engine` mode, or an edge that did not set it). The node asks which
tenant owns the domain. `tenant_id` on this request is empty.

```go
func (e *engine) domainLookup(ctx context.Context, fqdn string) *pb.LookupResponse {
	tid, err := e.rdb.Get(ctx, "domain:"+strings.ToLower(fqdn)).Result()
	switch {
	case err == redis.Nil:
		return &pb.LookupResponse{Found: false, TtlS: 30} // nobody owns it
	case err != nil:
		return &pb.LookupResponse{Unavailable: true} // backend down: NOT "not found"
	}
	return &pb.LookupResponse{Found: true, TtlS: 300, Payload: []byte(`{"tenant_id":"` + tid + `"}`)}
}
```

### 3.2 Directory: authenticate a user

**Situation:** a phone registers, or sends an INVITE the node must challenge.
The key is `user@domain`. Return a password **hash**, not the password.

```go
func a1(user, domain, password string) string { // md5(user:domain:password)
	sum := md5.Sum([]byte(user + ":" + domain + ":" + password))
	return hex.EncodeToString(sum[:])
}

func (e *engine) directoryLookup(ctx context.Context, tenant, key string) *pb.LookupResponse {
	h, err := e.rdb.HGetAll(ctx, "dir:"+tenant+":"+key).Result()
	if err != nil {
		return &pb.LookupResponse{Unavailable: true}
	}
	if len(h) == 0 {
		return &pb.LookupResponse{Found: false, TtlS: 30}
	}
	body, _ := json.Marshal(map[string]any{
		"name":    h["name"],
		"a1_hash": h["a1_hash"], // without it (or "password") the user can NOT authenticate
		"vars":    map[string]string{"accountcode": h["accountcode"]}, // only names in var-allow survive
	})
	ver, _ := strconv.ParseInt(h["ver"], 10, 64)
	return &pb.LookupResponse{Found: true, Version: ver, TtlS: 60, Payload: body}
}
```

### 3.3 Engine-served XML

**Situation:** you want to own a dialplan context, or a FreeSWITCH config file,
instead of keeping it on disk. Needs `xml-sections` on the node (see
[user-guide.md](user-guide.md) §5).

```go
func (e *engine) xmlLookup(ctx context.Context, r *pb.LookupRequest) *pb.LookupResponse {
	switch {
	case strings.HasPrefix(r.Key, "dialplan/"):
		// With xml-dialplan-fetch=call, r.Params holds the call's details; never cached.
		dest := r.Params["destination_number"]
		xml := `<document type="freeswitch/xml"><section name="dialplan"><context name="default">` +
			`<extension name="` + html.EscapeString(dest) + `"><condition field="destination_number" expression="^.*$">` +
			`<action application="grpc_route"/></condition></extension></context></section></document>`
		return &pb.LookupResponse{Found: true, Payload: []byte(xml)}
	case r.Key == "configuration/acl.conf":
		return &pb.LookupResponse{Found: true, TtlS: 300, Payload: []byte(e.aclXML())}
	}
	return &pb.LookupResponse{Found: false} // the node falls back to the file on disk
}
```

Escape every value you put into XML. Never copy caller-controlled text in
unescaped.

### 3.4 Survival snapshot

**Situation:** with `survival=true` the node keeps a copy of this document so it
can still bridge extension-to-extension calls while your engine is down.

```go
func (e *engine) survivalLookup(ctx context.Context, tenant string) *pb.LookupResponse {
	snap := map[string]any{
		"tenant_uuid": tenant,
		"domains":     []map[string]string{{"fqdn": "acme.example"}},
		"extensions": []map[string]any{
			{"domain": "acme.example", "number": "1001", "dnd": false, "enabled": true},
			{"domain": "acme.example", "number": "1002", "dnd": false, "enabled": true},
		},
	}
	b, _ := json.Marshal(snap)
	return &pb.LookupResponse{Found: true, TtlS: 300, Payload: b}
}
```

---

## 4. Routing: the first batch

`route` receives the `RouteRequest` and returns a batch. Each case below is a
branch of it.

```go
func (e *engine) route(r *pb.RouteRequest) *pb.RouteResponse {
	t, u, d := r.TenantId, r.Uuid, r.DestinationNumber
	if t == "" {
		return batch(hangup(t, u, "CALL_REJECTED"))
	}
	switch {
	case r.Vars["xfer_token"] != "":
		return e.routeTransferred(r)
	case r.Vars["dialer_token"] != "":
		return e.routeDialerLeg(r)
	case strings.HasPrefix(d, "10"): // extension
		return e.routeExtension(r)
	case d == "800":
		return e.routeMenu(r)
	case d == "801":
		return e.routeVoicemailDeposit(r, "1001")
	case d == "802":
		return e.routeConference(r)
	case d == "803":
		return e.routeQueue(r, "support")
	case d == "804":
		return e.routeSimpleRecord(r)
	case strings.HasPrefix(d, "+") || len(d) > 6:
		return e.routeOutbound(r)
	}
	return batch(
		cmd(t, &pb.Playback{Uuid: u, File: "ivr/ivr-no_route_destination.wav"}),
		hangup(t, u, "NO_ROUTE_DESTINATION"))
}
```

### 4.1 Ring an extension

The simplest case: bridge to a registered user.

```go
func (e *engine) routeExtension(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	return batch(cmd(t, &pb.Bridge{Uuid: u,
		DialString: "user/" + r.DestinationNumber + "@" + r.Domain, TimeoutS: 25}))
}
```

`user/<n>@<domain>` dials the user's registered contact (`local` and `engine`
mode add that automatically). Without a `step_id` the node bridges and the call
ends when either side hangs up.

### 4.2 Ring an extension, then voicemail if nobody answers

This version replaces 4.1's `routeExtension`. Put a `step_id` on the bridge so
you hear how it ended:

```go
func (e *engine) routeExtension(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	return batch(withStep(cmd(t, &pb.Bridge{Uuid: u,
		DialString: "user/" + r.DestinationNumber + "@" + r.Domain, TimeoutS: 20}),
		"ext:"+r.DestinationNumber))
}

// in step():  case strings.HasPrefix(s.StepId, "ext:"):
func (e *engine) afterExtension(s *pb.StepResult) *pb.RouteResponse {
	t, u := s.TenantId, s.Uuid
	b := s.GetBridge()
	if s.HungUp || (b != nil && b.Answered) {
		return &pb.RouteResponse{} // spoke to someone, or caller left: done
	}
	user := strings.TrimPrefix(s.StepId, "ext:")
	// b.Cause is USER_BUSY, NO_ANSWER, ... decide per cause if you like.
	return e.voicemailGreeting(t, u, user)
}
```

### 4.3 Menu (IVR)

```go
func (e *engine) routeMenu(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	return batch(
		cmd(t, &pb.Answer{Uuid: u}), // answer first: a menu is more than a short prompt
		withStep(cmd(t, &pb.Collect{Uuid: u, Prompt: "ivr/ivr-welcome.wav",
			MinDigits: 1, MaxDigits: 1, TimeoutMs: 6000, Terminators: "#"}), "menu"))
}

// in step():  case "menu":
func (e *engine) afterMenu(s *pb.StepResult) *pb.RouteResponse {
	t, u := s.TenantId, s.Uuid
	c := s.GetCollect()
	if c == nil || c.TimedOut {
		return batch(cmd(t, &pb.Playback{Uuid: u, File: "ivr/ivr-you_have_made_too_many_mistakes.wav"}),
			hangup(t, u, "NORMAL_CLEARING"))
	}
	switch c.Digits {
	case "1":
		return batch(withStep(cmd(t, &pb.Bridge{Uuid: u, DialString: "user/1001@acme.example", TimeoutS: 25}), "ext:1001"))
	case "2":
		return e.routeQueue(&pb.RouteRequest{TenantId: t, Uuid: u}, "support")
	case "9":
		return e.voicemailGreeting(t, u, "1001")
	}
	// invalid key: ask again. The same step id and the same digits always give this same batch.
	return batch(
		cmd(t, &pb.Playback{Uuid: u, File: "ivr/ivr-that_was_an_invalid_entry.wav"}),
		withStep(cmd(t, &pb.Collect{Uuid: u, Prompt: "ivr/ivr-welcome.wav",
			MinDigits: 1, MaxDigits: 1, TimeoutMs: 6000, Terminators: "#"}), "menu"))
}
```

To cap retries, put a counter in the step id (`"menu:2"`) and parse it in
`step`. The state then lives in the message, not in your server.

### 4.4 Collect a longer number (PIN or extension)

```go
withStep(cmd(t, &pb.Collect{Uuid: u, Prompt: "ivr/ivr-please_enter_pin_followed_by_pound.wav",
	MinDigits: 4, MaxDigits: 8, TimeoutMs: 8000, Terminators: "#"}), "pin")
```

`min_digits` can be 0, `max_digits` must be 1..32, `timeout_ms` must be
1..60000, and `terminators` only `0-9#*`. The terminator key is not included in
`CollectOutcome.digits`.

### 4.5 Voicemail

Two parts: leaving a message, then listening. Data layout is in
[building-an-engine.md](building-an-engine.md) §11. The node needs no
`mod_voicemail`.

**Leaving a message** (call to 801, or a no-answer fallthrough):

```go
func (e *engine) voicemailGreeting(t, u, user string) *pb.RouteResponse {
	greeting := e.presignGreeting(t, user) // presigned GET of a .wav on a media-hosts host
	return batch(
		cmd(t, &pb.Answer{Uuid: u}), // voicemail needs a real answer
		cmd(t, &pb.Playback{Uuid: u, Url: greeting}),
		withStep(cmd(t, &pb.Record{
			Uuid: u, Name: "vm-" + newID(), // newID: any unique id; local copy lands in record-dir/<tenant>/
			MaxLenS: 120, SilenceS: 5, Beep: true,
			UploadUrl: e.presignPut(t, "vm/"+t+"/"+user+"/"+newID()+".wav"), // FRESH object path each time
		}), "vmrec:"+user))
}

func (e *engine) routeVoicemailDeposit(r *pb.RouteRequest, user string) *pb.RouteResponse {
	return e.voicemailGreeting(r.TenantId, r.Uuid, user)
}

// in step():  case strings.HasPrefix(s.StepId, "vmrec:"):
func (e *engine) afterVoicemail(s *pb.StepResult) *pb.RouteResponse {
	user := strings.TrimPrefix(s.StepId, "vmrec:")
	if rec := s.GetRecord(); rec != nil && rec.Uploaded && rec.LengthMs > 1000 {
		e.storeMessage(s.TenantId, user, rec) // HSET message + ZADD to the "new" set
		e.pushMWI(s.TenantId, user)           // light the lamp (section 6.5)
	}
	return batch(
		cmd(s.TenantId, &pb.Playback{Uuid: s.Uuid, File: "voicemail/vm-goodbye.wav"}),
		hangup(s.TenantId, s.Uuid, "NORMAL_CLEARING"))
}
```

Never reuse an object path for different content: the node caches media by
host and path and ignores the query string.

**Listening** (a user dials `*98`): `Answer`, `Collect` the PIN, check it,
then loop with `Collect` + `Playback{Url}`. Fetch ten message ids at a time with
`ZRANGE ... BYSCORE`, as described in building-an-engine.md §11, and keep the
last score inside the `step_id`:

```go
// step id "vmmsg:<user>:<lastScore>" : play message after lastScore, then ask: 7 delete, 9 save, any other next
func (e *engine) vmNext(t, u, user, last string) *pb.RouteResponse {
	m, ok := e.nextMessage(t, user, last) // ZRANGE vm:{t}:msgs:{user}:new (last +inf BYSCORE LIMIT 0 1
	if !ok {
		return batch(cmd(t, &pb.Playback{Uuid: u, File: "voicemail/vm-no_more_messages.wav"}),
			hangup(t, u, "NORMAL_CLEARING"))
	}
	return batch(
		cmd(t, &pb.Playback{Uuid: u, Url: e.presignGet(t, m.Object)}),
		withStep(cmd(t, &pb.Collect{Uuid: u, Prompt: "voicemail/vm-message_options.wav",
			MinDigits: 1, MaxDigits: 1, TimeoutMs: 8000}), "vmmsg:"+user+":"+m.Score))
}
```

### 4.6 Hold queue (call center)

Data model and the full flow: building-an-engine.md §11. The routing part:

```go
func (e *engine) routeQueue(r *pb.RouteRequest, queue string) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	e.queueJoin(t, queue, u) // ZADD cc:{t}:q:{queue}
	return batch(
		cmd(t, &pb.Answer{Uuid: u}),                         // answered, so it is not dropped as unanswered while waiting
		cmd(t, &pb.Playback{Uuid: u, File: "local_stream://moh"}), // hold music, needs a stream in local_stream.conf
		withStep(cmd(t, &pb.Park{Uuid: u}), "queued:"+queue))
}
```

Then, from a background goroutine (not inside `route`), when an agent frees up,
**push a Bridge on the stream** (section 6.2):

```go
func (e *engine) dispatch(t, queue string) {
	caller, ok := e.queuePeek(t, queue) // the longest-waiting call, plus which node holds it
	if !ok {
		return
	}
	agent, ok := e.popAvailableAgent(t, queue) // ZPOPMIN cc:{t}:avail:{queue}
	if !ok {
		return
	}
	e.push(caller.NodeID, &pb.Command{CommandId: newID(), TenantId: t,
		Action: &pb.Command_Bridge{Bridge: &pb.Bridge{
			Uuid: caller.UUID, DialString: "user/" + agent.Ext + "@" + agent.Domain, TimeoutS: 20}}})
	// When the CommandResult arrives: ok -> ZREM caller, mark agent busy.
	//                                 error/no answer -> ZADD agent back with a penalty, try the next.
}
```

If the agent's phone is registered on a different node (`engine` mode), use
`PeerAddresses` and `PeerTarget` instead of `DialString` (section 4.9).

### 4.7 Conference room

```go
func (e *engine) routeConference(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	return batch(
		cmd(t, &pb.Answer{Uuid: u}),
		// joins room "<tenant>_team", and returns only when the caller leaves or hangs up
		cmd(t, &pb.Conference{Uuid: u, Room: "team", Profile: "default",
			Moderator: e.isModerator(t, r.CallerIdNumber), WaitForModerator: true}))
}
```

`Conference` blocks inline until the caller leaves. Controlling the room from
outside is a stream command: section 6.4.

### 4.8 Record the call and bridge

`Record` is a blocking inline command that records the caller's side of the
call into `<record-dir>/<tenant>/<name>.wav`. It is for prompt-and-record
flows such as a note or a greeting (voicemail above uses it too):

```go
func (e *engine) routeSimpleRecord(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	return batch(
		cmd(t, &pb.Answer{Uuid: u}),
		withStep(cmd(t, &pb.Record{Uuid: u, Name: "note-" + u, MaxLenS: 60, SilenceS: 4, Beep: true}), "recorded"))
}
// in step():  case "recorded": rec := s.GetRecord() -> rec.Path, rec.LengthMs
```

### 4.9 Multi-node: a user registered on another node

`engine` mode. Look up where the user's devices are registered (building-an-engine.md §8),
then:

```go
func (e *engine) routeRegistered(r *pb.RouteRequest, devs []regInfo, localNode string) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	user := r.DestinationNumber + "@" + r.Domain

	var local bool
	var peers []string
	for _, d := range devs {
		if d.NodeID == localNode {
			local = true
		} else {
			peers = append(peers, d.SipAddress)
		}
	}
	// The node already hopped this call once: local devices ONLY.
	if r.Vars["hop"] == "1" {
		peers = nil
	}
	b := &pb.Bridge{Uuid: u, TimeoutS: 25}
	if local {
		b.DialString = "user/" + user
	}
	if len(peers) > 0 {
		b.PeerAddresses, b.PeerTarget = peers, user
	}
	if b.DialString == "" && len(b.PeerAddresses) == 0 {
		return batch(hangup(t, u, "USER_NOT_REGISTERED"))
	}
	return batch(cmd(t, b))
}
```

### 4.10 Outbound call to the carrier

```go
func (e *engine) routeOutbound(r *pb.RouteRequest) *pb.RouteResponse {
	t, u := r.TenantId, r.Uuid
	if !e.mayCall(t, r.CallerIdNumber, r.DestinationNumber) { // permissions, balance, rate limit
		return batch(cmd(t, &pb.Playback{Uuid: u, File: "ivr/ivr-call_rejected.wav"}), hangup(t, u, "CALL_REJECTED"))
	}
	// no whitespace and no ${...} in dial strings; [var=value] block allowed once per leg
	dial := "[origination_caller_id_number=" + e.didFor(t) + "]sofia/gateway/carrier1/" + r.DestinationNumber
	return batch(withStep(cmd(t, &pb.Bridge{Uuid: u, DialString: dial, TimeoutS: 45}), "out"))
}
// in step():  case "out": b := s.GetBridge(); bill b.ElapsedS; b.Cause says why it ended.
```

For several carriers in order, join with `|`: `sofia/gateway/a/N|sofia/gateway/b/N`.
Ring several together with `,`.

### 4.11 Transfer (from engine-side logic)

Send `Transfer{Uuid, Token}` with a UUID-shaped token. The node transfers the
call to extension `xfer` in context `default`, and your dialplan must send that
to `grpc_route`. It returns as a new `RouteRequest` with `vars["xfer_token"]`:

```go
// 1. somewhere (a step reply or stream command):
tok := uuid.NewString()
e.pendingTransfers.Store(tok, "sales-queue") // remember what this token means
return batch(cmd(t, &pb.Transfer{Uuid: u, Token: tok}))

// 2. the call comes back:
func (e *engine) routeTransferred(r *pb.RouteRequest) *pb.RouteResponse {
	dest, ok := e.pendingTransfers.LoadAndDelete(r.Vars["xfer_token"])
	if !ok {
		return batch(hangup(r.TenantId, r.Uuid, "CALL_REJECTED")) // unknown or reused token
	}
	return e.routeByName(r, dest.(string))
}
```

```xml
<!-- dialplan on the node, context default -->
<extension name="xfer"><condition field="destination_number" expression="^xfer$">
  <action application="grpc_route"/></condition></extension>
```

### 4.12 Operator API route (`ApiBatch`)

**Situation:** instead of commands, set a header or run an allowed FreeSWITCH
API call. Only for nodes whose `Hello.tags` has `api-batch`, and only within the
node's `api-allow`, `api-headers`, `var-allow` and `xml-app-allow` lists.

```go
return &pb.RouteResponse{ApiBatch: &pb.ApiBatch{
	RuleId: "rule-42",
	Actions: []*pb.ApiAction{
		{Kind: pb.ApiAction_SET_HEADER, Name: "X-Campaign", Value: "spring"},
		{Kind: pb.ApiAction_SET_VAR, Name: "accountcode", Value: "acme"},
	}}}
```

### 4.13 Not routable / engine problem

```go
// backend down: temporary failure; with survival on, the node may still route extensions itself
return &pb.RouteResponse{Unavailable: true}

// valid but unroutable: say so and hang up with a precise cause
return batch(hangup(t, u, "UNALLOCATED_NUMBER"))
```

An empty batch with nothing else set **rejects** the call.

---

## 5. Multi-step calls

`step` receives each `StepResult` and returns the next batch. It dispatches on
`StepId`:

```go
func (e *engine) step(s *pb.StepResult) *pb.RouteResponse {
	if s.HungUp {
		return &pb.RouteResponse{} // the caller left: the reply is ignored
	}
	if s.Error != "" { // the node refused or could not run the command
		log.Printf("step %s on %s failed: %s", s.StepId, s.Uuid, s.Error)
		return batch(hangup(s.TenantId, s.Uuid, "NORMAL_TEMPORARY_FAILURE"))
	}
	id := s.StepId
	switch {
	case id == "menu":
		return e.afterMenu(s)
	case id == "recorded":
		return e.afterRecorded(s)
	case id == "pin":
		return e.afterPin(s)
	case id == "out":
		return e.afterOutbound(s)
	case id == "amd":
		return e.afterAmd(s)
	case strings.HasPrefix(id, "ext:"):
		return e.afterExtension(s)
	case strings.HasPrefix(id, "vmrec:"):
		return e.afterVoicemail(s)
	case strings.HasPrefix(id, "vmmsg:"):
		return e.afterVoicemailChoice(s)
	case strings.HasPrefix(id, "queued:"):
		return &pb.RouteResponse{} // parked; a dispatcher bridges this call later (4.6)
	}
	return batch(hangup(s.TenantId, s.Uuid, "NORMAL_CLEARING"))
}
```

Rules for a safe `step`:

- **Deterministic.** The same `step_id` and outcome must give the same batch:
  the node retries up to 6 times.
- **Keep state in the id.** Put small state in `step_id` (`vmmsg:1001:1763900000123456`).
  For larger state, use a shared store keyed by `call_id`, never process memory.
- **Always end.** A batch without a `step_id` ends the routing loop. Finish
  with `Hangup`, or a command that blocks until the call ends (`Bridge`,
  `Conference`).
- **Read `s.Vars`** if you set `route-vars` on the node: they are the values at
  the moment the step ended.

### Outbound dialer: answering-machine detection

**Situation:** place a call to a customer and act on human versus machine. Push
an `Originate` with `route_on_answer`. When the far end answers, the new leg
arrives as a `RouteRequest` with domain `dialer.invalid` and a `dialer_token`.

```go
// 1. start (stream command, section 6.1):
tok := uuid.NewString()
e.dialerCalls.Store(tok, campaign)
e.push(nodeID, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_Originate{Originate: &pb.Originate{
		DialString: "sofia/gateway/carrier1/" + number, TimeoutS: 30, RouteOnAnswer: true,
		Vars: map[string]string{"dialer_token": tok, "origination_uuid": newID(), "call_id": newID()}}}})

// 2. the answered leg returns:
func (e *engine) routeDialerLeg(r *pb.RouteRequest) *pb.RouteResponse {
	if _, ok := e.dialerCalls.LoadAndDelete(r.Vars["dialer_token"]); !ok {
		return batch(hangup(r.TenantId, r.Uuid, "CALL_REJECTED"))
	}
	return batch(withStep(cmd(r.TenantId, &pb.Amd{Uuid: r.Uuid}), "amd"))
}

// 3. decide:
func (e *engine) afterAmd(s *pb.StepResult) *pb.RouteResponse {
	t, u := s.TenantId, s.Uuid
	a := s.GetAmd() // result: HUMAN, MACHINE or NOTSURE; cause: why
	if a != nil && a.Result == "HUMAN" {
		return batch(withStep(cmd(t, &pb.Bridge{Uuid: u, DialString: "user/1001@acme.example", TimeoutS: 20}), "agent"))
	}
	return batch(
		cmd(t, &pb.Playback{Uuid: u, Url: e.presignGet(t, "campaigns/spring-message.wav")}),
		hangup(t, u, "NORMAL_CLEARING"))
}
```

The node accepts only the variables `call_id`, `origination_uuid` and
`dialer_token` on such a leg, and sends no `X-Tenant-Id` to the carrier.

---

## 6. Stream commands

Commands you send **on your own**, at any moment, not as the answer to a
request. You need to know which node holds the call (track `uuid` to `node_id`
from `RouteRequest` and `CallEvent`s) and keep the registry from
building-an-engine.md §4:

```go
func (e *engine) push(nodeID string, c *pb.Command) bool {
	v, ok := e.nodes.Load(nodeID)
	if !ok {
		return false
	}
	v.(*node).send(&pb.EngineMessage{Body: &pb.EngineMessage_Command{Command: c}})
	return true
}
```

Each command gets a `command_id`. The node answers with `CommandResult` carrying
that id. Blocking actions (`Collect`, `Record`, `Conference`, `Sleep`, `Amd`)
are refused here: they are inline only.

### 6.1 Click-to-call

```go
// ring agent 1001; when they pick up, hand the leg back to route() as a new call
e.push(nodeID, &pb.Command{CommandId: "c-" + newID(), TenantId: t,
	Action: &pb.Command_Originate{Originate: &pb.Originate{
		DialString: "user/1001@acme.example", TimeoutS: 30,
		Vars: map[string]string{"origination_caller_id_number": "1001"}}}})
```

### 6.2 Bridge or hang up a call you know about

```go
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_Hangup{Hangup: &pb.Hangup{Uuid: callUUID, Cause: "NORMAL_CLEARING"}}})

// join two existing calls
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_BridgeLegs{BridgeLegs: &pb.BridgeLegs{UuidA: a, UuidB: b}}})
```

The node refuses a command whose `tenant_id` does not own the `uuid`:
`no such call for this tenant`.

### 6.3 Supervisor: listen, whisper, barge

```go
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_Eavesdrop{Eavesdrop: &pb.Eavesdrop{
		Uuid: supervisorUUID, TargetUuid: agentCallUUID,
		Mode: pb.SuperviseModeKind_SUPERVISE_MODE_LISTEN}}})

// later, switch to whispering to the agent only, or to barging into the call
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_SuperviseMode{SuperviseMode: &pb.SuperviseMode{
		Uuid: supervisorUUID, Mode: pb.SuperviseModeKind_SUPERVISE_MODE_BARGE}}})
```

### 6.4 Control a conference from outside

```go
conf := func(a pb.ConferenceAction, member string) *pb.Command {
	return &pb.Command{CommandId: newID(), TenantId: t, Action: &pb.Command_ConferenceControl{
		ConferenceControl: &pb.ConferenceControl{Room: "team", Action: a, MemberUuid: member}}}
}
e.push(node, conf(pb.ConferenceAction_CONFERENCE_ACTION_MUTE_ALL, ""))   // spares moderators
e.push(node, conf(pb.ConferenceAction_CONFERENCE_ACTION_KICK, memberUUID)) // member must be in this tenant's room
e.push(node, conf(pb.ConferenceAction_CONFERENCE_ACTION_LOCK, ""))

// record the room and upload when done
e.push(node, &pb.Command{CommandId: newID(), TenantId: t, Action: &pb.Command_ConferenceControl{
	ConferenceControl: &pb.ConferenceControl{Room: "team",
		Action: pb.ConferenceAction_CONFERENCE_ACTION_RECORD_START,
		RecId:  "rec-1", MaxS: 3600, UploadUrl: e.presignPut(t, "conf/"+t+"/team/rec-1.wav")}}})
```

The node later sends `ConfEvent` `RECORDING_DONE` with `rec_id`, `length_ms`
and `uploaded`. `ROOM_READY` tells you the first member joined.

### 6.5 Message-waiting lamp

```go
// send to EVERY node holding a registration for the user (look them up first: building-an-engine.md §8)
func (e *engine) pushMWI(t, user string) {
	newMsgs, savedMsgs := e.vmCounts(t, user) // ZCARD of the two sets
	for _, nodeID := range e.nodesHolding(t, user) {
		e.push(nodeID, &pb.Command{CommandId: newID(), TenantId: t,
			Action: &pb.Command_MessageWaiting{MessageWaiting: &pb.MessageWaiting{
				User: user, Domain: "acme.example", NewMsgs: int32(newMsgs), SavedMsgs: int32(savedMsgs)}}})
	}
}
```

### 6.6 Announce to a call in progress

```go
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_Announce{Announce: &pb.Announce{
		Uuid: callUUID, Prompt: "ivr/ivr-your_call_is_recorded.wav", LegOnly: true}}})
```

### 6.7 Answer a call you hold

Rare, but valid on the stream path, and a no-op when the call is already
answered:

```go
e.push(callNode, &pb.Command{CommandId: newID(), TenantId: t,
	Action: &pb.Command_Answer{Answer: &pb.Answer{Uuid: callUUID}}})
```

---

## 7. Events

Events need no reply. Handle them in the `Connect` loop and update state.

```go
case *pb.NodeMessage_CallEvent:
	ev := b.CallEvent
	switch ev.Type {
	case pb.CallEventType_CALL_EVENT_TYPE_CREATED:
		e.calls.Store(ev.Uuid, callInfo{Tenant: ev.TenantId, Node: n.id, CallID: ev.CallId})
	case pb.CallEventType_CALL_EVENT_TYPE_ANSWERED:
		e.markAnswered(ev.Uuid, ev.Peer) // Peer: the number that leg dialled (e.g. a queue agent's extension)
	case pb.CallEventType_CALL_EVENT_TYPE_DTMF:
		e.onDigit(ev.Uuid, ev.Digit)
	case pb.CallEventType_CALL_EVENT_TYPE_HANGUP:
		e.calls.Delete(ev.Uuid)
		e.onHangup(ev.TenantId, ev.Uuid, ev.Cause) // billing, agent becomes available again
	}
case *pb.NodeMessage_Heartbeat:
	e.load.Store(n.id, b.Heartbeat) // active_calls, cpu_pct: choose nodes for Originate
case *pb.NodeMessage_ConfEvent:
	e.onConfEvent(b.ConfEvent) // RECORDING_DONE, ROOM_READY
case *pb.NodeMessage_CommandResult:
	e.onResult(b.CommandResult) // match command_id to what you pushed; retry on error if safe
```

Registrations (`RegEvent`) and the key/value store (`DataRequest`) are covered
with full Redis code in building-an-engine.md §8 and §10.

---

## 8. Operations

### 8.1 Invalidate after changing data

```go
func (e *engine) renameUser(ctx context.Context, t, user, domain, name string) {
	key := user + "@" + domain
	ver := e.rdb.Incr(ctx, "ver:"+t+":dir:"+key).Val() // 1. bump version
	e.rdb.HSet(ctx, "dir:"+t+":"+key, "name", name, "ver", ver)  // 2. write
	inv := &pb.EngineMessage{Body: &pb.EngineMessage_Invalidate{Invalidate: &pb.Invalidate{ // 3. tell every node
		TenantId: t, Kind: pb.LookupKind_LOOKUP_KIND_DIRECTORY, Key: key, Version: ver}}}
	e.nodes.Range(func(_, v any) bool { v.(*node).send(inv); return true })
}
```

Use `Key: "*"` for a whole tenant. Use `Kind: pb.LookupKind_LOOKUP_KIND_XML`
with `configuration/acl.conf` to make nodes reload their ACLs.

### 8.2 Drain a node for maintenance

```go
e.push("fs-01", &pb.Command{CommandId: newID(), TenantId: tenant, Action: &pb.Command_Drain{
	Drain: &pb.Drain{DeadlineS: 600}}})
// the node accepts no new inbound calls; existing ones finish. Reverse it on the node: `grpc undrain`.
```

### 8.3 Rotate a node's token

```go
newTok := e.mintToken()
e.storeToken(nodeID, newTok, 5*time.Minute) // accept BOTH old and new for the overlap
e.sendTo(nodeID, &pb.EngineMessage{Body: &pb.EngineMessage_TokenRotate{
	TokenRotate: &pb.TokenRotate{Token: newTok, OverlapS: 300}}})
```

The node writes the new token to its `token-file` and uses it from its next
`Hello`.

### 8.4 Fail honestly when a backend is down

| Request | Reply |
|---------|-------|
| `LookupRequest` | `LookupResponse{Unavailable: true}` (the node serves stale cache or fails) |
| `RouteRequest`, `StepResult` | `RouteResponse{Unavailable: true}` |
| `DataRequest` | `DataResponse{Unavailable: true}` |

Never reply `Found: false` or an empty batch for a failure: the first is cached
as a real "does not exist", the second rejects the call.

### 8.5 Do the work off the receive loop

```go
case *pb.NodeMessage_RouteRequest:
	req, id := b.RouteRequest, m.RequestId
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond) // node waits 3 s
		defer cancel()
		resp := e.routeCtx(ctx, req)
		if ctx.Err() != nil {
			resp = &pb.RouteResponse{Unavailable: true}
		}
		n.send(&pb.EngineMessage{RequestId: id, Body: &pb.EngineMessage_RouteResponse{RouteResponse: resp}})
	}()
```

---

## 9. Testing

**Without a phone.** Start a call from the node's console against your engine:

```
freeswitch> originate {tenant_id=<tenant-uuid>,domain_name=acme.example}null/800 &grpc_route()
```

`null/800` is a leg with no audio: it triggers `grpc_route` with destination
800 and exercises your logic and step loop.

**Look up a value through the node's cache.**

```
freeswitch> grpc tenant acme.example
freeswitch> grpc lookup <tenant-uuid> directory 1001@acme.example
freeswitch> grpc data set <tenant-uuid> counter:1 60 hello
```

**Unit-test the engine without FreeSWITCH.** `route` and `step` are pure
functions of their input, so test them as plain functions:

```go
func TestMenuPressOne(t *testing.T) {
	e := &engine{}
	got := e.step(&pb.StepResult{TenantId: tenant, Uuid: "u1", StepId: "menu",
		Outcome: &pb.StepResult_Collect{Collect: &pb.CollectOutcome{Digits: "1"}}})
	b := got.Commands[0].GetBridge()
	if b == nil || b.DialString != "user/1001@acme.example" {
		t.Fatalf("expected a bridge to 1001, got %v", got)
	}
	// retries must give the identical answer
	if again := e.step(&pb.StepResult{TenantId: tenant, Uuid: "u1", StepId: "menu",
		Outcome: &pb.StepResult_Collect{Collect: &pb.CollectOutcome{Digits: "1"}}}); !proto.Equal(got, again) {
		t.Fatal("step must be deterministic")
	}
}
```

**A fake node.** To test the whole `Connect` loop, write a small gRPC client
that sends `Hello`, a `LookupRequest` and a `RouteRequest`, and prints what the
engine returns. Anything that speaks the proto works, in any language.
