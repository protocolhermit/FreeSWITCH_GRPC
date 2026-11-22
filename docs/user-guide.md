# mod_grpc user guide

`mod_grpc` is a FreeSWITCH module that hands call control to an external
**engine** over one long-lived gRPC stream. FreeSWITCH stays the media and SIP
box; the engine decides who a user is, where a call goes, and what happens on
it (play, collect digits, bridge, record, conference, ...).

```
SIP edge (e.g. Kamailio) ──SIP──▶ FreeSWITCH + mod_grpc ══gRPC stream══▶ your engine
          sets X-Tenant-Id                │                              (routing, users,
                                          └─ media, bridging, recording   config, tenants)
```

- One FreeSWITCH node opens exactly **one** `Connect` stream to the engine.
- Everything (lookups, routing, events, commands) is multiplexed on that stream.
- The module is multi-tenant: every request carries a `tenant_id` (a UUID).

See [protocol.md](protocol.md) for the wire contract and
[building-an-engine.md](building-an-engine.md) to write the server side.

---

## 1. Build

### Requirements

| Need | Notes |
|------|-------|
| CMake ≥ 3.16, C99 and C++17 compiler | gcc or clang |
| gRPC and Protobuf (CMake config packages) | `find_package(gRPC CONFIG)` / `find_package(Protobuf CONFIG)` must work |
| libcurl | used to upload recordings to presigned URLs |
| An installed FreeSWITCH with `freeswitch.pc` | only needed to build the module itself |

### Repository layout

```
proto/controlplane/v1/control.proto   the wire contract (shared with the engine)
mod_grpc/bridge/                      libgrpc_bridge: gRPC transport, cache, reconnect, TLS (C++ behind a C API)
mod_grpc/module/                      mod_grpc.c: the FreeSWITCH module (C)
```

The build generates the C++ stubs from `proto/` itself; you don't run `protoc`
by hand.

### Build the module

```bash
cd mod_grpc
cmake -S . -B build-fs -G Ninja -DFS_PREFIX=/usr/local/freeswitch -DCMAKE_BUILD_TYPE=Release
cmake --build build-fs
sudo cmake --install build-fs      # copies mod_grpc.so to $FS_PREFIX/lib/freeswitch/mod
```

If you leave out `-DFS_PREFIX`, only the transport library (`libgrpc_bridge.a`)
and the proto library get built. The module is not.

Build notes:

- The module links libstdc++ statically and exports only
  `mod_grpc_module_interface` (`module/exports.map`). That keeps its gRPC and
  C++ runtime from clashing with other modules.
- It is linked `-z nodelete`, so `unload mod_grpc` does not unmap code that
  gRPC background threads are still running.

---

## 2. Configure

Create `autoload_configs/mod_grpc.conf.xml`:

```xml
<configuration name="mod_grpc.conf">
  <settings>
    <!-- required -->
    <param name="engine-endpoints" value="engine1.internal:9443,engine2.internal:9443"/>
    <param name="node-id"          value="fs-01"/>
    <param name="password"         value="REPLACE-WITH-NODE-TOKEN"/>
    <!-- or: <param name="token-file" value="/var/lib/freeswitch/grpc.token"/> -->

    <!-- TLS (strongly recommended; without tls-ca the stream is plaintext) -->
    <param name="require-tls"     value="true"/>
    <param name="tls-ca"          value="/etc/freeswitch/tls/ca.pem"/>
    <param name="tls-cert"        value="/etc/freeswitch/tls/node.pem"/>
    <param name="tls-key"         value="/etc/freeswitch/tls/node.key"/>
    <param name="tls-server-name" value="engine.internal"/>

    <param name="capacity"   value="500"/>
    <param name="record-dir" value="/var/lib/freeswitch/recordings"/>
  </settings>
</configuration>
```

Add `<load module="mod_grpc"/>` to `modules.conf.xml`, or run `load mod_grpc` from `fs_cli`.

### All settings

| Setting | Default | Meaning |
|---------|---------|---------|
| `engine-endpoints` | **required** | `host:port[,host:port...]`, tried round-robin on reconnect. |
| `node-id` | **required** | This node's name, sent in `Hello`. |
| `password` | **required** unless `token-file` | Node token sent in `Hello`. The sample value `change-me` is refused. |
| `token-file` | – | File holding the token. Takes precedence over `password`. When the engine rotates the token (`TokenRotate`), the module writes the new one here. |
| `tags` | – | Comma-separated tags sent in `Hello`. `api-batch` is always added. |
| `capacity` | `500` | Advertised capacity, sent in `Hello`. |
| `tls-ca` | – | CA bundle used to verify the engine. Unset means **plaintext**, so use it for development only. |
| `tls-cert`, `tls-key` | – | Client certificate for mTLS. Set both or neither. Both need `tls-ca`. |
| `tls-server-name` | endpoint host | Name to verify on the engine certificate. |
| `require-tls` | `false` | Refuse to load without `tls-ca`. |
| `apply-outbound-acl` | – | FreeSWITCH ACL list that each resolved engine IP must pass. |
| `keepalive-ms` | `30000` | HTTP/2 keepalive ping interval. |
| `hello-timeout-ms` | `10000` | How long to wait for `HelloAck`. |
| `cache-max-entries` | `10000` | Size of the lookup cache. |
| `record-dir` | – | Root for `Record`: files go to `<record-dir>/<tenant>/<name>.wav`. |
| `unavailable-prompt` | – | Played before hanging up when the engine can't be reached. |
| `route-vars` | `""` | Extra channel variables to send with `RouteRequest` and `StepResult`. |
| `api-allow` | `uuid_setvar,uuid_transfer,uuid_broadcast,originate` | FreeSWITCH API commands an engine `ApiBatch` may run. |
| `api-headers` | `""` | SIP headers an `ApiBatch` may set or unset. |
| `xml-app-allow` | `answer,set,export,playback,bridge,transfer,hangup,sleep` | Dialplan apps allowed in an `ApiBatch` XML fragment. |
| `var-allow` | `""` | Channel variables an `ApiBatch` may set. None until you list them. |
| `xml-sections` | – | Let the engine serve XML, for example `dialplan,configuration/acl.conf`. See §5. |
| `xml-dialplan-fetch` | `call` | `call`: ask on every dialplan hunt, never cached. `context`: fetch a whole context per tenant and cache it. |
| `survival` | `false` | Keep routing extension-to-extension calls from a cached snapshot while the engine is down. See §6. |
| `survival-dial-format` | – | Needed with `survival`: dial string with exactly two `%s` (number, domain), e.g. `sofia/internal/%s@%s`. |
| `survival-refresh` | `300` | Seconds between snapshot refreshes. |
| `survival-max-age` | `86400` | Seconds a snapshot may be served without an engine. |

The module **refuses to load** in these cases:

- a required setting is missing, or the password is still `change-me`
- `require-tls` is set without `tls-ca`
- only one of `tls-cert` and `tls-key` is set
- an allow-list names a command that is never permitted
- `survival` is on with a bad `survival-dial-format`

Check the FreeSWITCH log for the reason.

---

## 3. Send calls to the engine

Use the `grpc_route` dialplan application:

```xml
<extension name="engine">
  <condition field="destination_number" expression="^.*$">
    <action application="grpc_route"/>
  </condition>
</extension>
```

`grpc_route` does the following:

1. **Finds the tenant.** It reads the SIP header `X-Tenant-Id`
   (`sip_h_X-Tenant-Id`), set by your SIP edge, or the `tenant_id` channel
   variable. If neither is set, it asks the engine which tenant owns the call's
   domain (a `CONFIG` lookup of `domain:<fqdn>`).
2. Sets the `tenant_id` and `call_id` channel variables.
3. Sends a `RouteRequest`. It includes the caller, destination, domain, a few
   standard variables, every `X-*` SIP header except `X-Tenant-Id`, and your
   `route-vars`.
4. Runs the engine's answer: a batch of `Command`s. If the last command carries
   a `step_id`, the module reports the outcome (`StepResult`) and runs the next
   batch. This repeats until a batch has no `step_id` or the caller hangs up.
5. Hangs up with `NORMAL_TEMPORARY_FAILURE` if the engine is unreachable,
   says `unavailable`, or returns no commands. With survival on, it tries
   survival routing first.

The routing timeout is 3 s. Step reports are retried up to 6 times, 1 s apart,
and retrying is safe.

To test without SIP:

```
originate {tenant_id=<tenant-uuid>,domain_name=acme.test}null/1001 &grpc_route()
```

### Directory (SIP registration and auth)

The module answers FreeSWITCH `directory` lookups from the engine
(`LookupKind DIRECTORY`, key `user@domain`). The engine returns JSON such as
`{"name": "Alice"}`. The module builds the user entry with these variables:
`tenant_id`, `user_context=default`, `effective_caller_id_number`, and
`effective_caller_id_name`.

### Events

These channel events are streamed to the engine as `CallEvent`s on every
channel that has a `tenant_id`:

- `CHANNEL_CREATE`
- `CHANNEL_ANSWER`
- `DTMF`
- `CHANNEL_HANGUP_COMPLETE`

---

## 4. The `grpc` console command

| Command | What it does |
|---------|--------------|
| `grpc status` | Shows the connection, drain state, cache stats, survival on or off, and the last state change. |
| `grpc lookup <tenant> <directory\|dialplan\|config> <key>` | Runs a lookup through the cache and prints the payload. |
| `grpc cache flush` | Empties the lookup cache. |
| `grpc xml` | Shows the XML-fetch sections, mode, served and rejected counts, the last rejection, and the ACL reload state. |
| `grpc survival <tenant>` | Shows whether a survival snapshot is ready. |
| `grpc undrain` | Accepts new inbound calls again after an engine `Drain`. |

Example:

```
freeswitch> grpc status
node=fs-01 endpoints=engine1:9443 connected=yes draining=no cache_entries=12 hits=340 stale_served=0 invalidated=3 survival=off last="session 7f3c..."
```

---

## 5. Engine-served XML (optional)

With `xml-sections` set, the engine can serve FreeSWITCH XML fragments
(`LookupKind XML`):

- `dialplan` serves dialplan contexts. The key is `dialplan/<context>`. In
  `call` mode, every hunt sends the call's details as `params`.
- `configuration/<name>.conf` serves that configuration document. The key is
  `configuration/<name>.conf`. `mod_grpc.conf` itself is never fetched.
- `configuration/acl.conf` is special. The module reloads ACLs when it
  connects and whenever the engine invalidates that key. If an engine document
  would lock the node out of its own engine, it is rejected and the on-disk
  ACLs come back.

A section the engine doesn't serve (`found=false`) falls back to the files on
disk.

## 6. Survival mode (optional)

With `survival=true`, every tenant that routed a call through this node has its
`CONFIG` snapshot (key `survival`) fetched and refreshed every
`survival-refresh` seconds. When the engine is unreachable, a call is still
bridged if it meets all of these conditions:

- it targets an enabled, non-DND extension
- the extension is in one of the caller's own tenant domains
- the tenant comes from the `X-Tenant-Id` header

Calls to DIDs, groups, queues, outbound numbers, and feature codes are refused.
Callers on those calls hear `unavailable-prompt`, then the call hangs up.

## 7. Resilience

- **Reconnect:** exponential backoff from 0.5 s to 15 s, round-robin across
  endpoints.
- **Cache:** lookups are cached using the engine's `ttl_s` and dropped on
  `Invalidate`. After a reconnect, all entries are expired because
  invalidations may have been missed.
- **Fail-static:** if the engine can't be reached, an expired cache entry is
  served (`stale_served`) rather than failing.
- **Drain:** an engine `Drain` command pauses new inbound calls. Existing
  calls continue. `grpc undrain` or unloading the module lifts it.

## 8. Security notes

- Always run with TLS (`require-tls=true`), preferably mTLS, in production.
- Every engine command names a `tenant_id`. The module checks that it owns the
  channel `uuid` before acting. A mismatch gets "no such call for this tenant".
- Dial strings, prompts, room names, and record names are validated:
  - no `${...}` expansions
  - no path traversal
  - single path components only
- `ApiBatch` is limited to the `api-allow`, `api-headers`, `xml-app-allow`,
  and `var-allow` lists. Some commands are never allowed, whatever the config
  says.
