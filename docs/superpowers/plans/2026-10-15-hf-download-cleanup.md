# Interrupted HF download cleanup

**Goal:** Delete an interrupted shared parent even before its first child reference exists, using the original identity and paths across CR changes and retries.

**Design:** Persist a pending download on the child before filesystem writes. A successful attach atomically replaces that intent with the normal reference. Delete converts the intent to the existing pending deletion receipt while holding the parent and child locks, then uses the existing LockID-fenced cleanup. Failed and Ready parents are both eligible; recorded children, other pending downloads, local links, and reserved paths remain protected. No background GC or new parent status is introduced.

**Scope:** Work in the existing `fix-reuse-race` checkout. Keep cancellation status classification separate. Historical orphans without a recorded intent remain outside this change.

- [x] Add regressions for interrupted initial downloads, Ready-before-attach failures, and changed CR identity/path; verify failures against current code.
- [x] Record pending download identity/path/UID in the child ConfigMap before download or repair writes. Clear it atomically when attaching. Preserve it in routing, recovery, and final-deletion guards.
- [x] Convert pending downloads to existing deletion receipts under filesystem locks. Reuse parent ownership recovery and durable deletion LockIDs, including failed/lost API responses.
- [x] Handle pending downloads before source changes and opt-out; preserve pending sibling consumers and reserved paths.
- [x] Test lock contention, siblings, UID replacement, restart, API failures, idempotent retries, and ordinary successful download retry.
- [x] Preserve cross-directory parent reuse through attachment retries while still cleaning up changed identities, child paths, and UIDs.
- [x] Restore cached cleanup ownership when Delete precedes ConfigMap recovery, including loss after receipt publication and failed or lost recovery responses.
- [x] Run modelagent tests and focused race tests; inspect the final diff.

Validation: `go test ./pkg/modelagent -count=1` passed; `go test -race ./pkg/modelagent -run 'HfArtifact|GopherTask' -count=1` passed; `git diff --check` passed. Both test commands used the existing local Xet library through `DYLD_LIBRARY_PATH`. The initial regression run failed before implementation at the missing shared Delete handling.
