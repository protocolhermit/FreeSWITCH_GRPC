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

- [Getting started](docs/getting-started.md): new here? What mod_grpc is, how a call flows, first call in ten minutes
- [User guide](docs/user-guide.md): build, install, configure, dialplan, console commands
- [Protocol reference](docs/protocol.md): every message in `control.proto` and how it is used
- [Building an engine](docs/building-an-engine.md): write the server side, with a working Go example
- [Engine cookbook](docs/engine-examples.md): a Go engine snippet for every case: auth, routing, IVR, voicemail, queue, conference, outbound, supervision, MWI, operations

## Quick build

```bash
cd mod_grpc
cmake -S . -B build-fs -G Ninja -DFS_PREFIX=/usr/local/freeswitch
cmake --build build-fs && sudo cmake --install build-fs
```

## License

Copyright (C) 2026 Protocol Hermit. Licensed under the GNU General Public License, version 3 or (at your option) any later version. See [LICENSE](LICENSE).
