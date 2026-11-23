# TradeCore Adapter Provisioning Control API — Linux deployment gate

This directory deploys the non-hot-path account-provisioning service defined in
`docs/02-接口与数据/07-Adapter配置控制面实施与上游联调方案.md`.  It is separate from the
Consumer AF_UNIX socket and every OMS/EMS/Adapter execution process.  It does
not authorize `READY`, login, order submission, cancellation, or automated
recovery.

## Required protected inputs

The host operator must provide these **outside Git** under a root-owned,
`tradecore-control` readable-only configuration root (for example
`/etc/tradecore-control`, mode `0750`; individual private keys mode `0640`):

- TLS server certificate and private key; client CA bundle; TLS revocation
  policy; the listener accepts only TLS 1.3 and ALPN `h2`.
- certificate-fingerprint to `ControlPrincipalV1` allow-list, with independent
  `ADAPTER_PROVISIONER` and `APPROVER` identities;
- recipient public/private key pair and key rotation metadata for each actual
  secret host;
- an implementation of the deployment factory
  `tradecore_site.provisioning:create_server`, which returns exactly a
  `ProvisioningHttp2ServerV1` and reads protected inputs directly;
- a PostgreSQL DSN supplied only through a root-readable DSN file, a
  `PostgresProvisioningRegistryV1` and a
  `PostgresProvisioningApprovalRegistryV1`.  The runtime service account must
  have DML only; the migrator owns DDL.
- a production SecretProvider.  `LinuxProtectedFileSecretProviderV1` is a
  Linux/POSIX protected-file implementation with opaque references and short
  leases; installations requiring a KMS/HSM may replace it through the same
  contract.  The in-memory implementations are test fixtures and must never
  be selected by this factory.

Do not put broker account IDs, passwords, endpoints, JWE ciphertext, PEM
private keys, certificates, or database passwords in systemd `Environment=`,
the repository, command arguments, journald, SQL migration inputs, or this
directory.

## Install and preflight

1. Create the dedicated non-login service account `tradecore-control`; it must
   have no access to Consumer UDS, EMT runtime state, QMT Edge credentials, or
   OMS/EMS journal write directories.
2. Install the project virtual environment from the signed Git revision and
   verify `python -c "import h2; print(h2.__version__)"` within that venv.
3. Apply `sql/001_adapter_provisioning_v1.sql`, then
   `sql/002_adapter_provisioning_approval_facts_v1.sql`, with a dedicated
   migrator role.  The second migration preserves canonical wall-nanosecond
   values and durable maker-checker fields; it does not infer approval facts
   for old rows, which must be re-issued.
   Runtime service credentials receive only the least DML privileges required
   by the production repository; OMS/EMS/Consumer roles receive no privileges
   on `xuanling_tradecore_cp`.
4. Copy `../systemd/xuanling-tradecore-provisioning.service` to the host's
   unit directory only after replacing the non-secret factory module path if
   needed.  Do not add credentials to the unit.  Run `systemd-analyze verify`
   and verify all protective directives before enabling it.
5. Bind the service only to the approved administration network.  Firewall
   allow-list only configuration-client source identities; no data-plane,
   strategy, Consumer UDS, or broker endpoint may reach this listener.
6. Use a test CA to run the mutual TLS/HTTP2 contract test.  Verify rejection
   of TLS 1.2, non-`h2` ALPN, unknown client certificate, oversize body,
   plaintext credential JSON, JWE replay, wrong target, wrong role, unapproved
   activation, stale epoch, and retirement with unknown/working work.

7. Before enabling the unit, run the non-mutating
   `scripts/preflight.sh` as a deployment operator.  It verifies the dedicated
   account, root-owned protected paths, TLS input modes, CPython dependencies
   and systemd syntax.  Its successful result is only
   `CONTROL_PLANE_PREFLIGHT_OK_DISARMED`; it does not connect to a broker,
   provision an account or authorize `READY`.

The fixture test `tests/test_provisioning_control_plane_v1.py` proves the
repository listener contract with a temporary CA and client certificate.  It
does not certify a host's trust store, certificates, actual SecretProvider,
PostgreSQL role grants, EMT/QMT SDK, live Adapter, or `READY` state.  Those
host-specific facts must be captured as a separate deployment evidence set.

## Required S3 evidence before marking W2 accepted

Run the PostgreSQL contract test with a dedicated ephemeral test database by
setting `EMT_GATEWAY_TEST_POSTGRES_DSN`; it applies both migrations in a fresh
generated schema and proves durable profile/binding/approval round-trips.
Then, on the approved Linux control-plane node, archive a redacted evidence
manifest for migration/restart, mTLS negative matrix, secret revoke/rotation,
ACL/preflight, and a DISARMED activation/revoke sequence.  Do not put the DSN,
certificates, account aliases, secret references, endpoint values or JWE into
Git or that evidence manifest.
