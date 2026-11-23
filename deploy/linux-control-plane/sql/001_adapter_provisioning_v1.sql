-- TradeCore Provisioning Control Plane V1.
--
-- This migration is executed only by the dedicated control-plane migrator.
-- OMS/EMS/Adapter/Consumer processes must never hold DDL privileges or load
-- this file.  No table stores a password, broker account, endpoint, JWE
-- ciphertext, PEM key, TLS certificate, or reversible secret material.

CREATE SCHEMA IF NOT EXISTS xuanling_tradecore_cp;

CREATE DOMAIN xuanling_tradecore_cp.sha256_hex AS varchar(64)
    CONSTRAINT ck_control_sha256_hex_lowercase
    CHECK (VALUE ~ '^[0-9a-f]{64}$');

CREATE TABLE xuanling_tradecore_cp.adapter_profiles (
    profile_id text PRIMARY KEY,
    adapter_kind varchar(16) NOT NULL CHECK (adapter_kind IN ('EMT', 'QMT')),
    venue_id varchar(16) NOT NULL CHECK (venue_id IN ('EMT', 'QMT')),
    account_alias varchar(128) NOT NULL,
    current_revision bigint NOT NULL CHECK (current_revision >= 1),
    lifecycle varchar(32) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK (adapter_kind = venue_id)
);

CREATE TABLE xuanling_tradecore_cp.adapter_profile_revisions (
    profile_id text NOT NULL REFERENCES xuanling_tradecore_cp.adapter_profiles(profile_id),
    profile_revision bigint NOT NULL CHECK (profile_revision >= 1),
    adapter_instance_id text NOT NULL,
    state varchar(32) NOT NULL CHECK (
        state IN ('PREPARED', 'VALIDATED', 'ACTIVE_DISARMED', 'RETIRING', 'RETIRED', 'REJECTED')
    ),
    secret_provider_kind varchar(128) NOT NULL,
    secret_reference_id text NOT NULL,
    secret_target text NOT NULL,
    recipient_kid varchar(256) NOT NULL,
    credential_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    endpoint_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    envelope_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    mapping_manifest_hash xuanling_tradecore_cp.sha256_hex NOT NULL,
    capability_manifest_hash xuanling_tradecore_cp.sha256_hex NOT NULL,
    release_manifest_hash xuanling_tradecore_cp.sha256_hex NOT NULL,
    validation_evidence_id text,
    approval_id text,
    created_at timestamptz NOT NULL,
    retired_at timestamptz,
    PRIMARY KEY (profile_id, profile_revision),
    UNIQUE (adapter_instance_id),
    CHECK (
        (state = 'VALIDATED' AND validation_evidence_id IS NOT NULL)
        OR state <> 'VALIDATED'
    ),
    CHECK (
        (state = 'ACTIVE_DISARMED' AND validation_evidence_id IS NOT NULL AND approval_id IS NOT NULL)
        OR state <> 'ACTIVE_DISARMED'
    ),
    CHECK ((state = 'RETIRED' AND retired_at IS NOT NULL) OR (state <> 'RETIRED' AND retired_at IS NULL))
);

CREATE TABLE xuanling_tradecore_cp.control_request_ledger (
    request_id uuid PRIMARY KEY,
    principal_fingerprint xuanling_tradecore_cp.sha256_hex NOT NULL,
    payload_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    profile_id text NOT NULL,
    profile_revision bigint NOT NULL,
    operation varchar(32) NOT NULL,
    result_code varchar(96) NOT NULL,
    received_at timestamptz NOT NULL,
    UNIQUE (request_id, payload_digest),
    FOREIGN KEY (profile_id, profile_revision)
        REFERENCES xuanling_tradecore_cp.adapter_profile_revisions(profile_id, profile_revision)
);

CREATE TABLE xuanling_tradecore_cp.consumed_jwe_ids (
    recipient_target text NOT NULL,
    jti uuid NOT NULL,
    envelope_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz NOT NULL,
    PRIMARY KEY (recipient_target, jti)
);

CREATE TABLE xuanling_tradecore_cp.control_approvals (
    approval_id text PRIMARY KEY,
    approver_fingerprint xuanling_tradecore_cp.sha256_hex NOT NULL,
    profile_id text NOT NULL,
    profile_revision bigint NOT NULL,
    approval_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    FOREIGN KEY (profile_id, profile_revision)
        REFERENCES xuanling_tradecore_cp.adapter_profile_revisions(profile_id, profile_revision)
);

CREATE TABLE xuanling_tradecore_cp.consumer_binding_revisions (
    binding_revision_id text PRIMARY KEY,
    profile_id text NOT NULL,
    profile_revision bigint NOT NULL,
    account_id text NOT NULL,
    consumer_id text NOT NULL,
    consumer_account_id text NOT NULL,
    account_display_name text NOT NULL,
    strategy_ids jsonb NOT NULL,
    authorization_epoch bigint NOT NULL CHECK (authorization_epoch >= 1),
    effective_at timestamptz NOT NULL,
    retired_at timestamptz,
    FOREIGN KEY (profile_id, profile_revision)
        REFERENCES xuanling_tradecore_cp.adapter_profile_revisions(profile_id, profile_revision),
    UNIQUE (consumer_id, consumer_account_id, authorization_epoch)
);

CREATE TABLE xuanling_tradecore_cp.profile_validation_evidence (
    evidence_id text PRIMARY KEY,
    profile_id text NOT NULL,
    profile_revision bigint NOT NULL,
    evidence_digest xuanling_tradecore_cp.sha256_hex NOT NULL,
    passed boolean NOT NULL,
    reason_codes jsonb NOT NULL,
    evaluated_at timestamptz NOT NULL,
    FOREIGN KEY (profile_id, profile_revision)
        REFERENCES xuanling_tradecore_cp.adapter_profile_revisions(profile_id, profile_revision)
);

REVOKE ALL ON SCHEMA xuanling_tradecore_cp FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA xuanling_tradecore_cp FROM PUBLIC;
