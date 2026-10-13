# subplane-feeds: gateways that publish canonical SBE for the subplane agent

One binary, `subplane-gateway`, driven by per-exchange YAML packs (`packs/`). A pack describes the connection, the subscribe templates, how frames route (wrapper stream name or type/symbol fields, `each` for several records per frame) and the JSON path -> canonical field mapping (`path@conversion`, conversions named in `packs/registry.yaml`; models are the canonical schema itself). What is genuinely per-exchange (order-book sync protocols, auth, reconnect semantics) is a compiled-in strategy selected by name in the pack. Gateways publish one canonical SBE message per Aeron offer with one gap-free `Sequence`; the agent (`subplaned`, repo `subplane`) ingests it as an ordinary `aeron` partition.

## Contract with the agent
Only `contract/subplane_canonical.xml` (schema id 100) and its generated header, copied from the agent repo. No shared library. The agent never depends on this repo.

## Stack
C++20 (GCC 11, Ubuntu 22.04), CMake 3.30+, `-Werror`. Boost.Beast + OpenSSL (system) for WebSocket/REST, simdjson (fetched, pinned) for JSON, Aeron C client + media driver (fetched, pinned, same as the agent), yaml-cpp (system), doctest vendored.

## Docs
`docs/pack-format.md` (the mapping vocabulary), `docs/gateway.md` (threading, book strategies, ids, sequence, heartbeat, recording). Private notes in `.subplane/` (gitignored).

## Rules
No quick hacks; this is infrastructure. One I/O thread on the data path, no queue, no allocation per message after warm-up. `// INTENT:` comments where logic is non-obvious. Capture every design decision in `.subplane/decisions.md`. Record fixtures with `--record`, test with `--replay`, never with the live exchange in CI.
