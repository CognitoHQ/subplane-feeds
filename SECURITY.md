# Security

The gateways hold exchange connections and parse untrusted JSON from the
network; the snapshot service answers requests from agents. A bug in parsing,
book sync or the Aeron publication is a security bug.

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on
this repository. Do not open a public issue.

Include the commit, the pack and options, and the input that triggers it (a
recorded fixture is ideal). You will get an answer within a week, and a fix or
a plan before anything is disclosed.

## Supported versions

Pre-release: fixes land on `main` only.

## Scope

In scope: memory safety on any input (exchange WebSocket and REST responses,
packs, instrument files), the snapshot service's request handling, and the HTTP
endpoints doing more than documented. Out of scope: exchange-side behaviour,
and exposing the HTTP port to an untrusted network (bind `--http` to localhost
or a management network).
