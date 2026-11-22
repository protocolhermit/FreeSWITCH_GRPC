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
                                                            CallEvent, CommandResult, StepResult, ConfEvent } }
message EngineMessage { string request_id = 1; oneof body { HelloAck, LookupResponse, Invalidate,
                                                            RouteResponse, Command, TokenRotate } }
```

`request_id` ties a reply to its request. The engine **must echo the
`request_id`** of the node message it answers.

| Node sends | Engine replies with | Pattern |
|------------|--------------------|---------|
| `Hello` (always first) | `HelloAck` | handshake |
| `LookupRequest` | `LookupResponse` (same `request_id`) | request/response |
| `RouteRequest` | `RouteResponse` (same `request_id`) | request/response |
| `StepResult` | `RouteResponse` (same `request_id`): the next batch | request/response |
| `Heartbeat`, `CallEvent`, `ConfEvent` | nothing | fire-and-forget |
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
message Hello    { string node_id; string version; string token; repeated string tags; int32 capacity; string boot_id; }
message HelloAck { bool accepted; string reason; string session_id; int32 heartbeat_interval_s; }
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
| `DIRECTORY` | tenant | `user@domain` | `{"name":"Alice"}` (`found=false` rejects the user) |
| `CONFIG` | tenant | `survival` | Survival snapshot (see below) |
| `XML` | tenant or `""` | `dialplan/<context>` | A FreeSWITCH XML document for that context |
| `XML` | `""` | `configuration/<name>.conf` | A FreeSWITCH configuration document |
| `DIALPLAN`, other `CONFIG` | tenant | your choice | Free-form. You can test it with `grpc lookup`. |

Survival snapshot:

```json
{ "tenant_uuid": "…",
  "domains":    [ { "fqdn": "acme.example" } ],
  "extensions": [ { "domain": "acme.example", "number": "1001", "dnd": false, "enabled": true } ] }
```

Caching rules:

- `ttl_s` is how long the node may cache the answer.
- A request with `params` is never cached. This covers the per-call XML
  dialplan.
- `unavailable=true` means "I can't answer right now" (backend failure). It is
  **not** "not found". The node won't cache it and may serve a stale entry.
- `Invalidate` drops cache entries that match `tenant_id`, `kind`, and `key`.
  `kind=0` matches every kind. `key="*"` matches every key.
  - Send it whenever the data behind a key changes.
  - `Invalidate{tenant_id:"", kind:XML, key:"configuration/acl.conf"}` also
    makes the node reload its ACLs.

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
| `Bridge` | `uuid`, `dial_string`, `timeout_s`, `peer_tenant_id`, `peer_caller_id` | Bridges the call to a dial string |
| `BridgeLegs` | `uuid_a`, `uuid_b` | Bridges two existing legs |
| `Park` | `uuid` | Parks the call |
| `Playback` | `uuid`, `file` | Plays a file or stream |
| `Hangup` | `uuid`, `cause` | Hangs up with that cause |
| `Transfer` | `uuid`, `token` | `token` must be UUID-shaped. The node sets `xfer_token` and transfers the call to extension `xfer` in context `default`. The `default` dialplan must send it to `grpc_route`. The call then comes back as a new `RouteRequest` with `vars.xfer_token` |
| `Collect` | `uuid`, `prompt`, `min_digits`, `max_digits` (1..32), `timeout_ms` (1..60000), `terminators` (subset of `0-9#*`) | Plays a prompt and reads digits |
| `Record` | `uuid`, `name`, `max_len_s`, `silence_s`, `beep`, `upload_url` | Records to `<record-dir>/<tenant>/<name>.wav`, then optionally PUTs the file to a presigned URL |
| `Conference` | `uuid`, `room`, `profile`, `mute`, `moderator`, `wait_for_moderator` | Joins room `<tenant>_<room>` |
| `ConferenceControl` | `room`, `action`, `member_uuid`, `upload_url`, `rec_id`, `max_s`, `recording_prompt` | Stream only. Actions: mute, unmute, mute_all, unmute_all, kick, lock, unlock, end, record_start, record_stop |
| `Sleep` | `uuid`, `ms` (0..60000) | Waits |
| `Amd` | `uuid` | Answering-machine detection (mod_amd) |
| `Eavesdrop` | `uuid`, `target_uuid`, `mode` | Supervisor listens to, whispers to, or barges into a target call |
| `SuperviseMode` | `uuid`, `mode` | Switches the eavesdrop mode |
| `Announce` | `uuid`, `prompt`, `leg_only` | Plays a prompt to one or both legs |
| `Drain` | `deadline_s` | Stops accepting new inbound calls |

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
