# subplane-feeds: gateways that publish canonical SBE for the subplane agent

One binary, `spwsgw`, driven by per-exchange YAML packs (`packs/`). A pack describes the connection, the subscribe templates, how frames route (wrapper stream name or type/symbol fields, `each` for several records per frame) and the JSON path -> canonical field mapping (`path@conversion`, conversions named in `packs/registry.yaml`; models are the canonical schema itself). What is genuinely per-exchange (order-book sync protocols, auth, reconnect semantics) is a compiled-in strategy selected by name in the pack. Gateways publish canonical SBE messages over Aeron, one gap-free `Sequence` per message, the messages of one input frame back to back in one offer (schema-framed, no envelope, never a timer); the agent (`subplaned`, repo `subplane`) ingests it as an ordinary `aeron` partition.

## Contract with the agent
Only `contract/subplane_canonical.xml` (schema id 100) and its generated header, copied from the agent repo. No shared library. The agent never depends on this repo. The observability contract is copied too: `docs/observability.md` and `contract/topology-keys.json` (the agent's `docs/context/observability.md` and `docs/topology-keys.json`), with `src/obs/prom_writer.*` and `src/obs/endpoint_key.*` as copies of the agent's code; change them in the agent first.

## Stack
C++26 (GCC 16, Ubuntu 26.04), the agent's toolchain (its INV-007), CMake 3.30+, `-Werror`. Build, test and run in the toolchain image with `scripts/dev.sh` (`docker/Dockerfile`), never with the host compiler; CMake refuses GCC older than 16. With `../subplane/build` built by the agent's own `scripts/dev.sh build`, the end-to-end tests run against it. Boost.Beast + OpenSSL (system) for WebSocket/REST, simdjson (fetched, pinned) for JSON, Aeron C client + media driver (fetched, pinned, same as the agent), yaml-cpp (system), doctest vendored.

## Docs
`docs/pack-format.md` (the mapping vocabulary), `docs/gateway.md` (threading, book strategies, ids, sequence, heartbeat, recording). Private notes in `.subplane/` (gitignored).

## Rules
No quick hacks; this is infrastructure. One I/O thread on the data path, no queue, no allocation per message after warm-up. `// INTENT:` comments where logic is non-obvious. Capture every design decision in `.subplane/decisions.md`. Record fixtures with `scripts/record-fixture.sh`, test with `--replay`, never with the live exchange in CI; recordings are gitignored and never committed (exchange terms bar redistribution).
