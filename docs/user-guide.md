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
- A SIP edge is optional. Without Kamailio, the node can take phone
  registrations itself. See §9.

New to FreeSWITCH or gRPC? Read [getting-started.md](getting-started.md) first.
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
| `registration-mode` | `edge` | `edge`: a SIP edge (Kamailio) holds registrations and sets `X-Tenant-Id`. `local`: this node's sofia holds registrations, the tenant comes from the domain. `engine`: like `local`, and the node also reports registrations to the engine. See §9. |
| `registration-streams` | `1` | `engine` mode: how many `Registrations` streams carry registration events to the engine (1–16). Each user's events always use the same stream. Raise it for nodes with tens of thousands of registrations. An engine without that RPC gets them on the main stream instead (`reg_streams=0` in `grpc status`). |
| `tenancy` | `multi` | `multi`: tenant per call. `single`: every call, lookup, and directory entry uses `default-tenant`. |
| `default-tenant` | – | Tenant UUID. Required with `tenancy=single`. |
| `tenant-header-acl` | – | FreeSWITCH ACL list the SIP source must pass for its `X-Tenant-Id` to be trusted. In `local` and `engine` mode no header is trusted without it. In `edge` mode it is optional: unset trusts the header, as before. The list is not checked at load beyond a warning (FreeSWITCH can't tell a missing list from a denied address). The check probes `127.0.0.1`, so a peers-only list (see §9) always logs this WARNING at load; that is expected. |
| `directory-auth` | on in `local`/`engine`, off in `edge` | Put `a1_hash` or `password` from the engine's directory answer into the user entry, so sofia can challenge. When on, every user entry also gets `allow-empty-password=false`, so a user without credentials can't authenticate. Turning it off in `local`/`engine` mode logs a load-time WARNING: sofia then has no credentials to challenge with. |
| `peer-profile` | `internal` | Sofia profile used for peer hop legs (`[A-Za-z0-9_-]+`). |
| `sip-address` | – | `host:port` peers dial to reach this node (host is an IPv4 address or a name, `[A-Za-z0-9.-]`). **Required** in `engine` mode, and the module refuses to load if it is not `host:port`. Sent in `Hello`. |
| `data-timeout` | `1500` | Milliseconds a `grpc data` call waits for the engine. |
| `media-hosts` | – | Comma-separated host names `Playback.url` and `Collect.prompt_url` may fetch from. Bare names, no port, matched exactly and case-insensitively. Empty (the default) turns remote media off. |
| `media-cache-dir` | `<storage-dir>/grpc-media` | Where fetched media is kept. |
| `media-cache-max` | `1073741824` | Cache size cap in bytes. Over it, the oldest files go first (the file just fetched is never removed). A value below `media-max-bytes` is raised to it, with a warning. |
| `media-max-bytes` | `20971520` | Per-file download cap. |
| `media-fetch-timeout` | `10` | Seconds per download. |
| `upload-hosts` | – | Comma-separated host names a recording's `upload_url` (Record, conference recording) may PUT to, matched like `media-hosts`. A URL to any other host is refused (`record: bad upload url`). Empty (the default) allows any http or https host. |
| `<domain-tenants>` | – | A block next to `<settings>`: `<domain name="acme.example" tenant="<uuid>"/>`. Maps a domain to a tenant without asking the engine. Used in `local` mode only. |

The module **refuses to load** in these cases:

- a required setting is missing, or the password is still `change-me`
- `require-tls` is set without `tls-ca`
- only one of `tls-cert` and `tls-key` is set
- an allow-list names a command that is never permitted
- `survival` is on with a bad `survival-dial-format`
- `registration-mode` is not `edge`, `local`, or `engine`, or `tenancy` is not `single` or `multi`
- `tenancy=single` without a UUID `default-tenant`
- a `<domain>` in `<domain-tenants>` has no name or no UUID tenant
- `peer-profile` has characters outside `[A-Za-z0-9_-]`
- `registration-mode=engine` without `sip-address`

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

1. **Finds the tenant.** The order depends on the mode (table below). In the
   default `edge` mode it reads the SIP header `X-Tenant-Id`
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

Tenant resolution, first match wins:

| Mode | Order | No match |
|------|-------|----------|
| `edge` | `X-Tenant-Id` header (trusted unless `tenant-header-acl` is set and the source fails it), `tenant_id` variable, engine domain lookup | `tenant_id=""`, as before |
| `local` | header (UUID, and only if the source passes `tenant-header-acl`), `tenant_id` variable (UUID), `<domain-tenants>` map, engine domain lookup | call rejected |
| `engine` | same as `local`, without the map | call rejected |
| any, `tenancy=single` | `default-tenant`, always | – |

A rejected call logs `no tenant for domain …`, hangs up with
`NORMAL_TEMPORARY_FAILURE`, and sends no `RouteRequest`.

An `X-Tenant-Id` that is not trusted is removed from the channel, counted in
`untrusted_tenant_headers` (see `grpc status`), and neither used nor forwarded.
The XML dialplan fetch does not use it either.

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

With `directory-auth` on, the JSON can also carry:

| Field | Effect |
|-------|--------|
| `a1_hash` | Becomes the `a1-hash` param: `md5(user:domain:password)`. Preferred, so the engine never sends a clear password. |
| `password` | Becomes the `password` param. Used only when there is no `a1_hash`. |

With `directory-auth` on, the entry always gets `allow-empty-password=false`. A
user returned with neither `a1_hash` nor `password` therefore can't register or
place an authenticated call. Without this, sofia would accept any password for
that user, and the caller would get the user's `tenant_id`.

| Field | Effect |
|-------|--------|
| `vars` | Object of strings, set as user variables. Only names listed in `var-allow` are kept, and `tenant_id` can never be overridden. At most 32. |

In `local` and `engine` mode the entry also gets a `dial-string` domain param,
so `user/<n>@<domain>` dials the user's registered contact.

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
| `grpc tenant <domain>` | Shows which tenant a domain resolves to and where it came from, as `tenant=<uuid or -> source=<none, default, map, or engine>`. It uses no channel, so a header or variable never counts here. |
| `grpc data <op> <tenant> <key> ...` | Reads or writes the engine's key/value store. See below. |
| `grpc undrain` | Accepts new inbound calls again after an engine `Drain`. |

Example:

```
freeswitch> grpc status
node=fs-01 endpoints=engine1:9443 connected=yes draining=no cache_entries=12 hits=340 stale_served=0 invalidated=3 survival=off last="session 7f3c..." mode=edge tenancy=multi untrusted_tenant_headers=0 features=data,mwi,media_url data_ops=0 data_errors=0 media_fetches=0 media_cache_hits=0
```

`features` is what the engine listed in `HelloAck.features`. `data_ops` and
`data_errors` count `grpc data` calls and the ones that did not answer `+OK`.
`media_fetches` and `media_cache_hits` count remote media downloads and cache hits.

`mode`, `tenancy`, and `untrusted_tenant_headers` are always shown. In `engine`
mode the line also ends with `reg_streams=<n> reg_events_sent=<n> reg_events_dropped=<n> reg_resync_last=<unix time or never>`:
the registration streams open (0: events go on the main stream),
registration events sent, live events lost because the send queue was full,
and when the last replay ran.

### `grpc data`

A tenant-scoped key/value store kept by the engine. Use it for state that
several nodes share and change during calls, such as counters, locks and
queue positions.

```
grpc data get  <tenant> <key>
grpc data set  <tenant> <key> <ttl_s> <value>
grpc data del  <tenant> <key>
grpc data incr <tenant> <key> <delta> [<ttl_s>]
grpc data cas  <tenant> <key> <version> <ttl_s> <value>
```

`ttl_s` 0 means no expiry. For `incr`, the TTL applies only when it creates the
key. `cas` writes only if the key's version equals `<version>`; 0 means "only
if absent". The value is everything after the fixed arguments, so it may
contain spaces.

| Reply | Meaning |
|-------|---------|
| `+OK <version> <value>` | `get` found it, or `incr` (the value is the new integer). |
| `+OK <version>` | `set` or `cas` (the new version). |
| `+OK` | `del`, also when the key did not exist. |
| `-ERR not_found` | `get` found nothing. |
| `-ERR conflict <version>` | `cas`: the current version differs (0 = absent). |
| `-ERR unsupported` | The engine's `HelloAck.features` lacks `"data"`. |
| `-ERR unavailable`, `timeout`, `disconnected` | The engine's store, the engine, or the connection failed. |
| `-ERR bad_args`, `too_large`, `not_integer`, `bad_value` | Bad syntax or tenant, value over 64 KiB, `incr` on a non-integer, or a stored value that is not printable UTF-8. |

- `tenant` must be UUID-shaped. With `tenancy=single` it must be
  `default-tenant`. The module does not check who is calling: dialplan, ESL and
  modules are operator code, as with any other API command.
- Key: 1-256 printable ASCII bytes, no spaces. Value: UTF-8 without control
  characters, at most 64 KiB. Store JSON or base64 for anything else.
- **Values are never cached.** Every call goes to the engine. Data that rarely
  changes belongs in a `CONFIG` lookup, which is cached.
- The call blocks its thread until the engine answers or `data-timeout` passes.
  Do not call it from the event dispatch thread.
- An engine-served `ApiBatch` cannot run `grpc`: it is on the built-in deny
  list.

### Remote media and hold music

With `media-hosts` set, `Playback` and `Collect` can play a presigned URL
(see [protocol.md](protocol.md) §5). The node downloads it before playback, so
the caller hears silence meanwhile.

- Only `.wav`, `.mp3` and `.ogg`, from a listed host, over http or https.
  Redirects are not followed. A non-2xx answer is the error `fetch: http <code>`
  and is never cached.
- The cache key is the call's tenant, the host and the object path. The
  scheme, port and query string are ignored, so one object path must always
  hold the same content. Because the signature is not part of the key, each
  tenant downloads its own copy: one tenant never plays another's cached file.
  A call whose `tenant_id` is not UUID-shaped cannot play URLs
  (`fetch: no tenant`).
- URLs work only in a call's own route or step answer. On the stream path they
  are refused.
- `local_stream://<name>` is accepted in prompts. The stream itself comes from
  `local_stream.conf`, which `xml-sections` can serve from the engine.

### Message waiting

The engine's `MessageWaiting` command makes sofia NOTIFY the user's devices
registered on this node. `domain` must belong to the command's tenant, by the
same rules as `grpc tenant`.

### Engine-side call center, voicemail and fifo

A node whose engine runs call-center queues, voicemail or fifo must **not**
load `mod_callcenter`, `mod_voicemail` or `mod_fifo`. The engine does that work
(see [building-an-engine.md](building-an-engine.md) §11), and a second
implementation on the node would fight it for the same calls.

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

---

## 9. Running without Kamailio

By default a SIP edge such as Kamailio holds phone registrations and tells the
node the tenant in `X-Tenant-Id`. With `registration-mode=local` or `engine`,
the FreeSWITCH node does both itself.

| Mode | Registrations | Tenant | Multi-node |
|------|---------------|--------|------------|
| `edge` | at the edge | `X-Tenant-Id` from the edge | the edge routes |
| `local` | in this node's sofia | from the domain (§3) | none: a user reachable on one node only |
| `engine` | in this node's sofia, and reported to the engine as `RegEvent` | from the domain (§3) | the engine knows which node holds each device and answers a `Bridge` with `peer_addresses` |

Use `engine` when a user's phone may be registered on any of several nodes.
The engine side is in
[building-an-engine.md](building-an-engine.md) §8.

### Node settings (`engine` mode)

```xml
<param name="registration-mode" value="engine"/>
<param name="tenancy"           value="multi"/>
<param name="tenant-header-acl" value="peers"/>
<param name="sip-address"       value="10.0.0.11:5080"/>
```

`peers` is a FreeSWITCH ACL listing the other nodes (`acl.conf`). It doesn't
list `127.0.0.1`, so the load-time WARNING "tenant-header-acl peers denies
127.0.0.1 or does not exist" is expected here:

```xml
<list name="peers" default="deny">
  <node type="allow" cidr="10.0.0.11/32"/>
  <node type="allow" cidr="10.0.0.12/32"/>
</list>
```

`local` mode needs no `sip-address`. Single-tenant installs set `tenancy=single`
and `default-tenant`. Local mode can map domains without the engine:

```xml
<domain-tenants>
  <domain name="acme.example" tenant="6f1c2a8e-3b7d-4c11-9a0e-2d5f6b7c8d90"/>
</domain-tenants>
```

### Sofia profile (`local` and `engine`)

On the profile that phones register on (usually `internal`):

| Param | Value | Why |
|-------|-------|-----|
| `disable-register` | `false` | Accept registrations. |
| `auth-calls` | `true` | Challenge calls from phones. |
| `challenge-realm` | `auto_from` | Digest uses the SIP domain as realm, so `a1_hash` is `md5(user:domain:password)`. Without it an INVITE is challenged with the node's IP as realm and fails. REGISTER works either way. |
| `apply-inbound-acl` | the `peers` list | Peers calling in with a hop leg skip the digest challenge. Phones are not on the list, so they are still challenged. |

Peers must be on **both** lists: sofia `apply-inbound-acl` (so the hop leg is
accepted) and mod_grpc `tenant-header-acl` (so its `X-Tenant-Id` is trusted).
Add the dialplan in §3 to the profile's context.

Registration data lives in each node's sofia memory. A shared ODBC database
between nodes is **not** supported: every node would see every other node's
registrations as its own.

### What the node does in `engine` mode

- **Registrations.** `sofia::register`, `sofia::unregister`, and `sofia::expire`
  become `RegEvent`s. After the engine stream reconnects, the node replays
  its live registrations (from `sofia xmlstatus`) with `resync=true`.
  They travel on their own `Registrations` streams (`registration-streams`),
  so a replay never holds up calls. A live event that finds its send queue full
  is dropped. The node counts it
  (`reg_events_dropped`), logs a WARNING at most every 10 s, and schedules a
  replay, which restores a lost `REGISTER`. A lost `UNREGISTER` or `EXPIRE` is
  not replayed: the engine keeps that device until its `expires_s` runs out.
- **Peer legs.** A `Bridge` with `peer_addresses` and `peer_target` rings the
  local device and each listed node together. Each hop leg is
  `sofia/<peer-profile>/<user@domain>;fs_path=sip:<host:port>` with `X-Tenant-Id` and
  `X-Grpc-Hop=1` added by the module. `fs_path` has no angle brackets: the
  bracket form loses the user and domain on FreeSWITCH 1.11.
- **Loop guard.** A hop that arrives with `X-Grpc-Hop` from a trusted peer
  reaches the engine as `RouteRequest.vars["hop"]="1"`. The engine must route it
  to local devices only. The node also refuses a peer `Bridge` on such a call.

### Limits

- A caller that offers only L16 (for example an `originate` on `loopback/`) can't use the peer hop: the peer answers 488. Real SIP callers can. Test the hop from a SIP phone or a second FreeSWITCH.
- A replayed `expires_s` may be longer than the phone's real `Expires` by up to sofia's late margin (about a minute). The engine should treat `expires_s` as a TTL.
- IPv6 peer addresses are refused.
