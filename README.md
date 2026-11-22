# freeswitch_grpc

`mod_grpc` is a FreeSWITCH module that connects each node to an external
control **engine** over one bidirectional gRPC stream. The engine handles:

- user directory
- call routing
- multi-step IVR flows
- configuration
- live call control

Calls, conferences, recordings, and supervision are multi-tenant. The module
runs behind a SIP edge such as Kamailio or on its own, with phones registering
on the FreeSWITCH nodes.

| Path | Contents |
|------|----------|
| `mod_grpc/` | The module (`module/`) and its gRPC transport library (`bridge/`) |
| `proto/controlplane/v1/control.proto` | The wire contract shared with the engine |
| `docs/` | Documentation |

## Docs

- [User guide](docs/user-guide.md): build, install, configure, dialplan, console commands
- [Protocol reference](docs/protocol.md): every message in `control.proto` and how it is used
- [Building an engine](docs/building-an-engine.md): write the server side, with a working Go example

## Quick build

```bash
cd mod_grpc
cmake -S . -B build-fs -G Ninja -DFS_PREFIX=/usr/local/freeswitch
cmake --build build-fs && sudo cmake --install build-fs
```
