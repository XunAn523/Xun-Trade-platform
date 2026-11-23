-- TradeCore Provisioning Control Plane V1 — durable approval facts.
--
-- Apply only through the dedicated control-plane migrator, after
-- 001_adapter_provisioning_v1.sql.  The application service holds no DDL
-- privilege.  These columns contain only approval identity and timing; they
-- never contain a credential, endpoint, JWE, certificate, or private key.

ALTER TABLE xuanling_tradecore_cp.control_approvals
    ADD COLUMN IF NOT EXISTS action varchar(16),
    ADD COLUMN IF NOT EXISTS approver_id text,
    ADD COLUMN IF NOT EXISTS issued_at_wall_ns bigint,
    ADD COLUMN IF NOT EXISTS expires_at_wall_ns bigint;

-- PostgreSQL ``timestamptz`` is microsecond-resolution.  Preserve the
-- control-plane's canonical monotonic wall-nanosecond values separately so
-- idempotent receipts, binding generations and retirement evidence do not
-- silently lose precision on a durable round-trip.
ALTER TABLE xuanling_tradecore_cp.adapter_profile_revisions
    ADD COLUMN IF NOT EXISTS created_wall_ns bigint,
    ADD COLUMN IF NOT EXISTS retired_wall_ns bigint;

ALTER TABLE xuanling_tradecore_cp.consumer_binding_revisions
    ADD COLUMN IF NOT EXISTS effective_wall_ns bigint,
    ADD COLUMN IF NOT EXISTS retired_wall_ns bigint;

UPDATE xuanling_tradecore_cp.adapter_profile_revisions
SET created_wall_ns = floor(extract(epoch FROM created_at) * 1000000000)::bigint
WHERE created_wall_ns IS NULL;

UPDATE xuanling_tradecore_cp.adapter_profile_revisions
SET retired_wall_ns = floor(extract(epoch FROM retired_at) * 1000000000)::bigint
WHERE retired_at IS NOT NULL AND retired_wall_ns IS NULL;

UPDATE xuanling_tradecore_cp.consumer_binding_revisions
SET effective_wall_ns = floor(extract(epoch FROM effective_at) * 1000000000)::bigint
WHERE effective_wall_ns IS NULL;

UPDATE xuanling_tradecore_cp.consumer_binding_revisions
SET retired_wall_ns = floor(extract(epoch FROM retired_at) * 1000000000)::bigint
WHERE retired_at IS NOT NULL AND retired_wall_ns IS NULL;

-- Existing pre-V1 rows lack the action/principal/wall-clock facts necessary
-- for maker-checker validation.  They remain historical rows but cannot be
-- used by the new repository: operators must issue a fresh approval rather
-- than having a migration infer authorization.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_control_approval_action_v1'
          AND connamespace = 'xuanling_tradecore_cp'::regnamespace
    ) THEN
        ALTER TABLE xuanling_tradecore_cp.control_approvals
            ADD CONSTRAINT ck_control_approval_action_v1
            CHECK (action IS NULL OR action IN ('VALIDATE', 'ACTIVATE', 'RETIRE'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_control_approval_interval_v1'
          AND connamespace = 'xuanling_tradecore_cp'::regnamespace
    ) THEN
        ALTER TABLE xuanling_tradecore_cp.control_approvals
            ADD CONSTRAINT ck_control_approval_interval_v1
            CHECK (
                (issued_at_wall_ns IS NULL AND expires_at_wall_ns IS NULL)
                OR (
                    issued_at_wall_ns >= 0
                    AND expires_at_wall_ns > issued_at_wall_ns
                )
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_control_approvals_exact_v1
    ON xuanling_tradecore_cp.control_approvals (profile_id, profile_revision, action, expires_at_wall_ns);

-- Control requests are immutable audit facts.  The runtime role must receive
-- INSERT/SELECT only; deny UPDATE/DELETE explicitly after role names are
-- substituted by the operator's reviewed grant script.
