# The control-plane protocol (`proto/controlplane/v1/control.proto`)

This is the contract between `mod_grpc` (the **node**) and your **engine**.
Package `controlplane.v1`, one service:

```proto
service ControlPlane {
  rpc Connect(stream NodeMessage) returns (stream EngineMessage);
}
```

The node is the gRPC **client** and the engine is the **server**. Each
FreeSWITCH node keeps exactly one bidirectional `Connect` stream open.
Everything below travels as `NodeMessage` (node → engine) or `EngineMessage`
(engine → node) on that stream.

**Tenant convention:** every `tenant_id` on the wire is the tenant's UUID
string. An empty `tenant_id` means a node-level request, for example a domain
→ tenant lookup.

---

## 1. Message envelopes and correlation

```proto
message NodeMessage   { string request_id = 1; oneof body { Hello, Heartbeat, LookupRequest, RouteRequest,
                                                            CallEvent, CommandResult, StepResult, ConfEvent, RegEvent,
                                                            DataRequest } }
message EngineMessage { string request_id = 1; oneof body { HelloAck, LookupResponse, Invalidate,
                                                            RouteResponse, Command, TokenRotate, DataResponse } }
```

`request_id` ties a reply to its request. The engine **must echo the
`request_id`** of the node message it answers.

| Node sends | Engine replies with | Pattern |
|------------|--------------------|---------|
| `Hello` (always first) | `HelloAck` | handshake |
| `LookupRequest` | `LookupResponse` (same `request_id`) | request/response |
| `RouteRequest` | `RouteResponse` (same `request_id`) | request/response |
| `StepResult` | `RouteResponse` (same `request_id`): the next batch | request/response |
| `Heartbeat`, `CallEvent`, `ConfEvent`, `RegEvent` | nothing | fire-and-forget |
| `CommandResult` | nothing (it *is* the reply to an engine `Command`) | ack |

| Engine sends unprompted | Node does |
|-------------------------|-----------|
| `Command` | Runs it on the node's command thread, then sends `CommandResult{command_id}` |
| `Invalidate` | Drops matching cache entries |
| `TokenRotate` | Stores the new token and uses it on the next `Hello` |

A reply that arrives after the node's timeout is dropped silently. Node-side
timeouts are:

- route and step: 3 s
- directory and domain lookups: 1.5 s
- XML fetch: 1.5 s

---

## 2. Handshake and liveness

```proto
message Hello    { string node_id; string version; string token; repeated string tags; int32 capacity; string boot_id;
                    string sip_address; string registration_mode; repeated string features; }
message HelloAck { bool accepted; string reason; string session_id; int32 heartbeat_interval_s; repeated string features; }
message Heartbeat{ int32 active_calls; int32 cpu_pct; int64 ts_unix_ms; }
```

- The first message on a stream **must** be `Hello`. The node waits
  `hello-timeout-ms` (10 s by default) for `HelloAck` and drops the stream
  if none arrives.
- `token` is the node's credential, from `password` or `token-file`.
  Authenticate it. Send `accepted=false` with a `reason` to refuse the node.
  The node logs `hello refused: <reason>` and retries with backoff.
- `tags` always includes `api-batch`. That tag tells you the node understands
  `RouteResponse.api_batch`.
- `boot_id` identifies one FreeSWITCH process. A new `boot_id` means the
  process restarted, so drop any calls you believed were on that node. An
  empty `boot_id` means the build doesn't report one. Don't treat it as a
  restart.
- `sip_address` (7) is the `host:port` peers dial to reach this node's SIP
  profile (the `sip-address` setting). Empty when unset.
- `registration_mode` (8) is `"local"` or `"engine"`. It is **empty in `edge`
  mode**, so an edge node's `Hello` is unchanged.
- `Hello.features` (9) lists the optional capabilities the node serves:
  `"answer"` (it runs `Command.answer`). Check for it before relying on
  `Answer`. An older node leaves it empty: inline it fails `Answer` with
  `unknown command type 0`, and on the stream it drops the command without
  a `CommandResult`.
- `HelloAck.features` (5) lists the optional capabilities the engine serves: `"data"`
  (it answers `DataRequest`), `"media_url"` (it sends `Playback.url` and
  `Collect.prompt_url`) and `"mwi"` (it sends `MessageWaiting`). Absent means
  none. Only `"data"` changes node behaviour: without it, `grpc data` answers
  `-ERR unsupported` at once instead of waiting out `data-timeout` for a reply
  an older engine never sends. The node shows the list as `features=` in
  `grpc status`.
- `heartbeat_interval_s` sets how often the node sends `Heartbeat`. `0` means
  every 5 s.

---

## 3. Lookups and cache invalidation

```proto
enum LookupKind { LOOKUP_KIND_DIRECTORY = 1; LOOKUP_KIND_DIALPLAN = 2; LOOKUP_KIND_CONFIG = 3; LOOKUP_KIND_XML = 4; }
message LookupRequest  { string tenant_id; LookupKind kind; string key; map<string,string> params; }
message LookupResponse { bool found; int64 version; int32 ttl_s; bytes payload; bool unavailable; }
message Invalidate     { string tenant_id; LookupKind kind; string key; int64 version; }
```

`payload` is opaque to the transport. The module expects the following
payloads:

| kind | tenant_id | key | payload the module expects |
|------|-----------|-----|---------------------------|
| `CONFIG` | `""` | `domain:<fqdn lowercased>` | `{"tenant_id":"<uuid>"}`: which tenant owns the domain |
| `DIRECTORY` | tenant | `user@domain` | `{"name":"Alice"}` (`found=false` rejects the user). Optional: `a1_hash`, `password`, `vars` (below). |
| `CONFIG` | tenant | `survival` | Survival snapshot (see below) |
| `XML` | tenant or `""` | `dialplan/<context>` | A FreeSWITCH XML document for that context |
| `XML` | `""` | `configuration/<name>.conf` | A FreeSWITCH configuration document |
| `DIALPLAN`, other `CONFIG` | tenant | your choice | Free-form. You can test it with `grpc lookup`. |

Directory JSON fields:

| Field | Type | Effect |
|-------|------|--------|
| `name` | string | Caller-ID name. Defaults to the user. |
| `a1_hash` | string | `md5(user:domain:password)`, set as the `a1-hash` param. Preferred. |
| `password` | string | `password` param. Used only when `a1_hash` is absent. |
| `vars` | object of strings | User variables, filtered by `var-allow`. `tenant_id` is never overridable. At most 32. |

`a1_hash` and `password` are applied only when `directory-auth` is on (the
default in `local` and `engine` mode). With `directory-auth` on, the user entry
always carries `allow-empty-password=false`: a user the engine returns without
`a1_hash` or `password` can't register or authenticate a call at all, with any
password. (sofia's own default would accept any password for such a user.)
The sofia `dial-string` domain param is added whenever `registration-mode` is
not `edge`, whatever `directory-auth` says.

Survival snapshot:

```json
{ "tenant_uuid": "…",
  "domains":    [ { "fqdn": "acme.example" } ],
  "extensions": [ { "domain": "acme.example", "number": "1001", "dnd": false, "enabled": true } ] }
```

Caching rules:

- `ttl_s` is how long the node may cache the answer. A not-found answer is
  cached for at most 30 s, whatever `ttl_s` says. A not-found with `ttl_s=0`
  is still cached briefly: 30 s for `XML`, 5 s for every other kind. That way
  a scanner retrying an unknown user or domain costs one round trip per window.
  Send an `Invalidate` when you create a user, if 5 s is too long to wait.
- A request with `params` is never cached. This covers the per-call XML
  dialplan.
- `unavailable=true` means "I can't answer right now" (backend failure). It is
  **not** "not found". The node won't cache it and may serve a stale entry.
- `Invalidate` drops cache entries that match `tenant_id`, `kind`, and `key`.
  `kind=0` matches every kind. `key="*"` matches every key.
  - Set `version` to the new data's version. For 60 s after an exact-key
    `Invalidate` (a wildcard one: for good), the node refuses to cache an
    older answer for that key, so a lookup already in flight can't bring the
    stale value back. Answers with `version=0` are exempt.
  - Send it whenever the data behind a key changes.
  - `Invalidate{tenant_id:"", kind:XML, key:"configuration/acl.conf"}` also
    makes the node reload its ACLs.

### Data requests

```proto
message DataRequest  { string tenant_id; Op op; string key; string value; int32 ttl_s; int64 delta; int64 expect_version;
                       enum Op { UNSPECIFIED = 0; GET = 1; SET = 2; DEL = 3; INCR = 4; CAS = 5; } }
message DataResponse { bool ok; bool found; bool conflict; bool unavailable; string value; int64 version; string error; }
```

`NodeMessage.data_request` (11) and `EngineMessage.data_response` (8). The node
sends a `DataRequest` for each `grpc data` command (see
[user-guide.md](user-guide.md) §4). Correlate by `request_id`, as with a
lookup. The node never caches a `DataResponse`.

| Op | Uses | Answer |
|----|------|--------|
| `GET` | `key` | `ok`, and `found` with `value` and `version`. A missing key is `ok=true, found=false`. |
| `SET` | `key`, `value`, `ttl_s` | `ok` with the new `version`. `ttl_s=0` means no expiry. |
| `DEL` | `key` | `ok`, also when the key did not exist. |
| `INCR` | `key`, `delta`, `ttl_s` | `ok` with `value` = the new integer and its `version`. A missing key counts as 0, and `ttl_s` applies only when `INCR` creates it. A value that is not an integer gives `ok=false, error="not_integer"`. |
| `CAS` | `key`, `value`, `ttl_s`, `expect_version` | Writes only when the key's version equals `expect_version` (0 = only if absent). On success `ok` with the new `version`. On a mismatch `conflict=true` and `version` = the current one (0 = absent). |

- `tenant_id` is always set, and the engine keeps keys under that tenant only.
- `unavailable=true` means the engine could not reach its store.
- `version` is a per-key counter. Every write bumps it, so a `CAS` is safe
  across nodes.
- `key` is 1-256 printable ASCII bytes without spaces. `value` is UTF-8 with
  no control characters, at most 64 KiB. The node checks both before sending.

---

## 4. Routing a call

```proto
message RouteRequest  { string tenant_id; string call_id; string uuid; string domain;
                        string caller_id_number; string destination_number; map<string,string> vars; }
message RouteResponse { repeated Command commands; bool unavailable; ApiBatch api_batch; }
```

`grpc_route` sends `RouteRequest`:

- `uuid` is the FreeSWITCH channel UUID. Commands use it to name the call.
- `call_id` is the logical call ID. It is the `call_id` variable when set,
  otherwise the uuid.
- `vars` includes:
  - `sip_call_id`, `network_addr`, `sip_from_user`, `sip_from_host`
  - `xfer_token`, `dialer_token`, `grpc_api_rule`, when set
  - every `sip_h_X-*` header except `X-Tenant-Id`
  - the variables listed in `route-vars`

The engine answers in one of three ways:

- **`commands`**: a batch run in order on this call (see §5).
- **`unavailable=true`**: the node hangs up with a temporary failure, or uses
  survival routing.
- **`api_batch`**: an "operator API route". Sent only to nodes tagged
  `api-batch`. The node runs these `ApiAction`s instead of commands:

| `ApiAction.Kind` | What the node does | Gate |
|------------------|--------------------|------|
| `SET_HEADER` / `UNSET_HEADER` | Sets or removes SIP header `name` | `api-headers` |
| `SET_VAR` | Sets channel variable `name=value` | `var-allow` |
| `API` / `BGAPI` | Runs FreeSWITCH API `name value` | `api-allow` |
| `XML` | Runs `value`, a dialplan XML fragment, in context `grpc_api` | `xml-app-allow`; `${...}` expansion is refused |

An empty `commands` list with no `api_batch` and no `unavailable` rejects the
call.

### Multi-step calls: `step_id` and `StepResult`

Put a `step_id` on the **last** command of a batch when you want to know how
it ended. The node runs the batch, then sends:

```proto
message StepResult { string tenant_id; string call_id; string uuid; string step_id;
                     oneof outcome { BridgeOutcome; CollectOutcome; PlaybackOutcome; RecordOutcome;
                                     ConferenceOutcome; SleepOutcome; EavesdropOutcome; AmdOutcome; }
                     string error; bool hung_up; map<string,string> vars; }
```

Any command can carry the `step_id`. The eight in the `oneof` report their
outcome. Any other command (`Park`, `Hangup`, `Transfer`, …) gets a
`StepResult` with no outcome set: it says only that the command ended (or
`error`, if it failed).

You reply with a `RouteResponse` carrying the next batch, using the **same
`request_id`**. This repeats until you send a batch without a `step_id`, or
the caller hangs up.

- `error` set: the command couldn't run. For example, a validation failure or
  a missing file.
- `hung_up=true`: the caller is gone. Your reply is ignored, so send an empty
  `RouteResponse`.
- The node retries a step up to 6 times, so **handle the same `step_id` the
  same way every time**. Return the same next batch.

Outcomes:

| Outcome | Fields |
|---------|--------|
| `BridgeOutcome` | `answered`, `cause` (e.g. `USER_BUSY`, `NO_ANSWER`), `elapsed_s` |
| `CollectOutcome` | `digits`, `timed_out` |
| `RecordOutcome` | `path`, `length_ms`, `uploaded` |
| `AmdOutcome` | `result` (`HUMAN`/`MACHINE`/`NOTSURE`), `cause` |
| others | empty |

---

## 5. Commands

```proto
message Command { string command_id; string tenant_id; oneof action { … }; string step_id; }
```

`tenant_id` is required. The node checks that the tenant owns every `uuid` the
command names.

There are two ways a command arrives:

- **Inline**: inside a `RouteResponse`, running on the call that asked. Only
  inline commands may use the blocking actions: `Collect`, `Record`,
  `Conference`, `Sleep`, and `Amd`.
- **Stream**: as an unprompted `EngineMessage.command`, for calls or the node
  generally. The node replies `CommandResult{command_id, ok, error}`. It
  refuses the blocking actions on this path.

| Action | Fields | Effect |
|--------|--------|--------|
| `Originate` | `dial_string`, `vars`, `timeout_s`, `route_on_answer` | Places a new call. The leg carries `X-Tenant-Id`. With `route_on_answer`, the answered leg comes back to you as a fresh `RouteRequest` (domain `dialer.invalid`, identified by `dialer_token`) instead of being parked. |
| `Bridge` | `uuid`, `dial_string`, `timeout_s`, `peer_tenant_id`, `peer_caller_id`, `peer_addresses`, `peer_target` | Bridges the call to a dial string. See "Peer legs" below. |
| `BridgeLegs` | `uuid_a`, `uuid_b` | Bridges two existing legs |
| `Answer` | `uuid` | Answers the call (200 OK). Already answered: ok, nothing happens. `Playback`, `Collect` and `Record` only pre-answer (early media), so send `Answer` first for anything longer than a short prompt: carrier timers drop an unanswered call after 60 to 180 s, many devices send no audio before answer, and billing starts at answer. |
| `Park` | `uuid` | Parks the call |
| `Playback` | `uuid`, `file`, `url` | Plays a file or stream. `url` replaces `file`: see "Remote media" below. |
| `Hangup` | `uuid`, `cause` | Hangs up with that cause |
| `Transfer` | `uuid`, `token` | `token` must be UUID-shaped. The node sets `xfer_token` and transfers the call to extension `xfer` in context `default`. The `default` dialplan must send it to `grpc_route`. The call then comes back as a new `RouteRequest` with `vars.xfer_token` |
| `Collect` | `uuid`, `prompt`, `prompt_url`, `min_digits`, `max_digits` (1..32), `timeout_ms` (1..60000), `terminators` (subset of `0-9#*`) | Plays a prompt (or `prompt_url`) and reads digits |
| `Record` | `uuid`, `name`, `max_len_s`, `silence_s`, `beep`, `upload_url` | Records to `<record-dir>/<tenant>/<name>.wav`, then optionally PUTs the file to a presigned URL |
| `Conference` | `uuid`, `room`, `profile`, `mute`, `moderator`, `wait_for_moderator` | Joins room `<tenant>_<room>` |
| `ConferenceControl` | `room`, `action`, `member_uuid`, `upload_url`, `rec_id`, `max_s`, `recording_prompt` | Stream only. Actions: mute, unmute, mute_all, unmute_all, kick, lock, unlock, end, record_start, record_stop |
| `Sleep` | `uuid`, `ms` (0..60000) | Waits |
| `Amd` | `uuid` | Answering-machine detection (mod_amd) |
| `Eavesdrop` | `uuid`, `target_uuid`, `mode` | Supervisor listens to, whispers to, or barges into a target call |
| `SuperviseMode` | `uuid`, `mode` | Switches the eavesdrop mode |
| `Announce` | `uuid`, `prompt`, `leg_only` | Plays a prompt to one or both legs |
| `MessageWaiting` | `user`, `domain`, `new_msgs`, `saved_msgs`, `new_urgent`, `saved_urgent` | Stream only. Lights or clears the message-waiting lamp: see below. |
| `Drain` | `deadline_s` | Stops accepting new inbound calls |

### Remote media

`Playback.url` (3) and `Collect.prompt_url` (7) are presigned `http(s)` GET
URLs of a `.wav`, `.mp3` or `.ogg`. They exist so a voicemail recorded on one
node (`Record` with `upload_url`) can play on another.

- A URL replaces `file` or `prompt`. Setting both is an error:
  `playback: url and file`, `collect: url and file`.
- The node fetches only from hosts in `media-hosts` (empty = off), without
  following redirects, and caches the file locally (user-guide §2). A failed
  fetch is the command's `error`: `fetch: no media-hosts`, `fetch: not allowed`,
  `fetch: http <code>`, `fetch: too large`, `fetch: timeout`. A non-2xx answer
  is never cached.
- **Inline only.** The download blocks the call's thread and the caller hears
  silence meanwhile, so the stream path refuses a URL (`playback: url is inline
  only`, and the same for `Announce`).
- The cache key is the host and object path, **not** the query string or
  scheme. The engine must never reuse an object path for different content.
- `Playback.file` and the other prompts also accept `local_stream://<name>`,
  where `<name>` is one safe component. It plays a hold-music stream from the
  node's `local_stream.conf`.

### `MessageWaiting`

Stream only, because it names no call. The node fires a message-waiting event
for `user@domain`, and sofia sends a NOTIFY to that user's devices registered
**on this node**. Send it to every node that holds a registration for the user.
A node with none does nothing.

- `domain` must resolve to the command's `tenant_id`, or the result is
  `mwi: domain not in tenant`. `user` and `domain` must be safe names, or
  `mwi: bad user or domain`. `user` may start with one `+` (E.164, e.g.
  `+15551234567`). A negative count gives `mwi: bad counts`.
- Messages waiting is `yes` when `new_msgs` or `new_urgent` is above 0.

### Peer legs (`engine` mode)

`Bridge.peer_addresses` (6, `repeated string`) lists `host:port` SIP addresses
of nodes that hold the callee's registrations. `Bridge.peer_target` (7) is
`user@domain` for the hop legs. `dial_string` may be empty when the callee has
no local device.

The node rings `dial_string` and one hop leg per address together. Each hop leg
is `[sip_h_X-Tenant-Id=<tenant>,sip_h_X-Grpc-Hop=1]sofia/<peer-profile>/<peer_target>;fs_path=sip:<host:port>`.
The module adds both headers itself.

The node refuses the `Bridge` (the result carries an `error`) when:

- `registration-mode` is not `engine`
- the channel already carries `X-Grpc-Hop` (no second hop)
- `peer_tenant_id` is also set
- `dial_string` contains `|`
- an address is not `host:port` (host `[A-Za-z0-9.-]`, port 1..65535, no IPv6)
- `peer_target` is not exactly `user@domain`

A hop leg that reaches another node sends a `RouteRequest` with
`vars["hop"]="1"`. The module sets `hop` only in `engine` mode, when
`X-Grpc-Hop` is present and its `X-Tenant-Id` was trusted. The engine must then
bridge to local devices only.

## 6. Events

```proto
message CallEvent { string tenant_id; string call_id; string uuid; CallEventType type; string digit;
                    string cause; int64 ts_unix_ms; string peer; string peer_tenant_id; }
message ConfEvent { Type type /* RECORDING_DONE | ROOM_READY */; string tenant; string room; string rec_id;
                    int64 length_ms; bool uploaded; string member_uuid; string room_instance; }
```

| `CallEvent` type | Extra fields |
|------------------|--------------|
| `CREATED` | – |
| `ANSWERED` | `peer`: the number the leg dialled |
| `DTMF` | `digit` |
| `HANGUP` | `cause` |

`ConfEvent RECORDING_DONE` reports a conference recording that
`ConferenceControl` started, including whether its upload succeeded.

### Registrations

```proto
message RegEvent { Type type /* REGISTER=1 | UNREGISTER=2 | EXPIRE=3 */; string tenant_id; string user; string domain;
                   string contact; int32 expires_s; string network_ip; int32 network_port;
                   string user_agent; string call_id; bool resync; }
```

Sent only in `engine` mode. Fire-and-forget, no reply. It travels on a
`Registrations` stream when the engine implements that RPC (below), otherwise
as `NodeMessage.reg_event` (field 10) on `Connect`.

| Field | Meaning |
|-------|---------|
| `type` | `REGISTER` (also a refresh), `UNREGISTER`, or `EXPIRE`. |
| `tenant_id` | Tenant of the domain (resolved as in user-guide §3). |
| `user`, `domain` | The address of record. |
| `contact` | The raw Contact header. It may carry a display name. |
| `expires_s` | Seconds until the registration expires. |
| `network_ip`, `network_port`, `user_agent` | Where the phone registered from. |
| `call_id` | The REGISTER's Call-ID: one device. Use it to match an unregister to its register. |
| `resync` | `true` on a replay after the node reconnects to the engine. |

After every reconnect the node replays the live registrations on its profiles
with `resync=true`. A replayed `expires_s` may exceed the phone's `Expires` by
up to sofia's late margin. A replay can also re-add a device that unregistered
while the replay ran, so a live `UNREGISTER` (`resync=false`) should win over an
older `resync` `REGISTER` for the same `call_id`.

#### The `Registrations` streams

```proto
rpc Registrations(stream RegStreamMessage) returns (stream RegStreamAck);
message RegStreamMessage { oneof body { RegStreamOpen open = 1; RegEvent event = 2; } }
message RegStreamOpen    { string node_id; string token; string session_id; int32 shard; int32 shards; }
message RegStreamAck     { bool accepted; string reason; }
```

Each stream has its own send queue on the node, and its own HTTP/2 flow
control. A burst of registrations (a replay of thousands, or an engine that is
slow to store them) therefore never waits ahead of, or fills the queue for,
`RouteRequest` and `StepResult` on `Connect`.

1. After an accepted `HelloAck`, and before it reports the stream up, a node in
   `engine` mode opens `registration-streams` of these (default 1, at most 16).
   Each sends `RegStreamOpen` first, with `shard` 0 to `shards-1` and the
   `session_id` from the `HelloAck`.
2. The engine checks `token` as it checks Hello's, and that `session_id` is
   that node's live session. It then answers one `RegStreamAck` and sends
   nothing more on the stream.
3. The node sends each `RegEvent` on shard `hash(user@domain) % shards` (the
   domain lowercased). All events for one address of record are on one
   stream, in order. Different streams are not ordered relative to each other.
   An engine may handle each stream on its own worker.
4. The streams belong to the session. If one ends, the node closes the whole
   session, reconnects, and replays. When the `Connect` stream ends, the node
   closes them too.

Fallback: when the engine answers `UNIMPLEMENTED` (it does not have the RPC),
or `accepted=false`, or the stream fails before the ack, the node opens none
for that session. `RegEvent`s then go on `Connect` as before. `grpc status`
shows `reg_streams=0`. The whole exchange counts against `hello-timeout`.
On `Connect`, a replayed event is refused while that queue is half full, so
the replay always leaves room for calls.

`RegEvent` is fire-and-forget. A live event that finds its send queue (the
shard's, or `Connect`'s in fallback)
full is lost, counted as `reg_events_dropped` in `grpc status`, and a replay is
scheduled. The replay restores a lost `REGISTER`. A lost `UNREGISTER` or
`EXPIRE` is not replayed, so that device stays until its `expires_s` runs out.

## 7. Token rotation

```proto
message TokenRotate { string token; int32 overlap_s; }
```

Send `TokenRotate` to replace a node's credential. The node:

1. Writes the new token to `token-file`, if one is configured.
2. Uses it on its next `Hello`.

Keep the old token valid for `overlap_s` seconds.

## 8. Evolving the proto

- Never renumber or reuse field numbers. Only add new ones.
- The node and the engine generate code from the same file. Keep one copy,
  this repo's `proto/`, and generate the engine's stubs from it.
