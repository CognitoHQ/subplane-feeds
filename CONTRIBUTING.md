# Contributing

subplane-feeds follows the engineering rules of the agent it feeds: read
[subplane's CONTRIBUTING](https://github.com/CognitoHQ/subplane/blob/main/CONTRIBUTING.md)
first. In short: correctness and stability over velocity, no heap or locks on a
gateway's hot path, `// INTENT:` comments on critical code, and docs that mark
what is built and what is planned.

## Workflow

1. Build and test in the toolchain image, as in subplane (`scripts/dev.sh test`).
2. A new venue is a YAML pack plus a recorded fixture (`fixtures/<pack>/`)
   replayed in `ctest`; see the README's pack table.
3. One logical change per commit; explain the why in the message body.
4. Open a pull request against `main`. CI must be green.

## Reporting

- **Bugs and features**: open an issue.
- **Security**: privately, never in a public issue; see `SECURITY.md`.
- **Conduct**: see `CODE_OF_CONDUCT.md`.

## Licence and CLA

subplane-feeds is licensed under the Apache License 2.0 (see `LICENSE`).
Before a first contribution is merged, its author signs the Individual
Contributor License Agreement (`CLA.md`) by adding a row to
`CLA-signatures.md` in their pull request; CI checks that the author of every
pull request has one. If your employer has rights to what you write, it signs
the Corporate CLA (`CLA-corporate.md`) first.
