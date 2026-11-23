-- F3 initial read model.  Applied only by the explicit tradecore_migrator
-- deployment command; it is never loaded by an execution or callback path.

CREATE SCHEMA IF NOT EXISTS xuanling_tradecore_rm;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tradecore_projector') THEN
        CREATE ROLE tradecore_projector NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tradecore_query') THEN
        CREATE ROLE tradecore_query NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tradecore_audit') THEN
        CREATE ROLE tradecore_audit NOLOGIN;
    END IF;
END
$$;

CREATE DOMAIN xuanling_tradecore_rm.sha256_hex AS varchar(64)
    CONSTRAINT ck_sha256_hex_lowercase
    CHECK (VALUE ~ '^[0-9a-f]{64}$');

CREATE TABLE xuanling_tradecore_rm.accounts (
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    consumer_id uuid NOT NULL,
    account_display_name varchar(128) NOT NULL,
    venue_account_masked varchar(64),
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    lifecycle varchar(32) NOT NULL,
    reconciliation_status varchar(32) NOT NULL,
    reconciliation_reason_code varchar(64),
    active_gate_count integer NOT NULL,
    can_submit boolean NOT NULL,
    submit_block_reason_code varchar(64),
    last_recovery_run_id uuid,
    capability_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    updated_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_accounts PRIMARY KEY (account_id, venue_id, adapter_instance_id),
    CONSTRAINT uq_accounts_consumer_scope
        UNIQUE (consumer_id, account_id, venue_id, adapter_instance_id),
    CONSTRAINT ck_accounts_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_accounts_active_gate_count CHECK (active_gate_count >= 0),
    CONSTRAINT ck_accounts_lifecycle
        CHECK (lifecycle IN ('BOOTSTRAP', 'RECOVERING', 'DISARMED', 'READY')),
    CONSTRAINT ck_accounts_reconciliation_status
        CHECK (reconciliation_status IN ('NOT_REQUIRED', 'REQUIRED', 'RUNNING', 'VALIDATED')),
    CONSTRAINT ck_accounts_submit_ready
        CHECK (
            NOT can_submit
            OR (lifecycle = 'READY' AND reconciliation_status IN ('NOT_REQUIRED', 'VALIDATED')
                AND active_gate_count = 0)
        ),
    CONSTRAINT ck_accounts_submit_reason
        CHECK (
            (can_submit AND submit_block_reason_code IS NULL)
            OR (NOT can_submit AND submit_block_reason_code IS NOT NULL)
        ),
    CONSTRAINT ck_accounts_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.strategies (
    strategy_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    strategy_code varchar(128) NOT NULL,
    display_name varchar(256) NOT NULL,
    strategy_kind varchar(16) NOT NULL,
    status varchar(16) NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    updated_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_strategies PRIMARY KEY (strategy_id),
    CONSTRAINT uq_strategies_consumer_id UNIQUE (consumer_id, strategy_id),
    CONSTRAINT uq_strategies_consumer_code UNIQUE (consumer_id, strategy_code),
    CONSTRAINT ck_strategies_kind CHECK (strategy_kind IN ('PROGRAM', 'MANUAL')),
    CONSTRAINT ck_strategies_status CHECK (status IN ('ACTIVE', 'DISABLED', 'REVOKED')),
    CONSTRAINT ck_strategies_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.orders (
    oms_order_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    strategy_id uuid NOT NULL,
    strategy_order_id varchar(128) NOT NULL,
    trader_id uuid NOT NULL,
    origin varchar(16) NOT NULL,
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    market varchar(32) NOT NULL,
    instrument_id varchar(64) NOT NULL,
    side varchar(16) NOT NULL,
    position_effect varchar(16),
    order_type varchar(32) NOT NULL,
    time_in_force varchar(32),
    currency char(3) NOT NULL,
    order_qty numeric(24, 8) NOT NULL,
    limit_price numeric(24, 8),
    filled_qty numeric(24, 8) NOT NULL,
    leaves_qty numeric(24, 8) NOT NULL,
    avg_fill_price numeric(24, 8),
    status varchar(48) NOT NULL,
    risk_result varchar(32) NOT NULL,
    risk_reason_code varchar(64),
    decision_context_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    business_remark varchar(512),
    intent_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    terminal_at timestamptz,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY (oms_order_id),
    CONSTRAINT uq_orders_strategy_idempotency UNIQUE (strategy_id, strategy_order_id),
    CONSTRAINT uq_orders_execution_scope
        UNIQUE (
            oms_order_id, account_id, venue_id, adapter_instance_id, trading_day, trading_day_epoch
        ),
    CONSTRAINT fk_orders_account_scope FOREIGN KEY (
        consumer_id, account_id, venue_id, adapter_instance_id
    ) REFERENCES xuanling_tradecore_rm.accounts (
        consumer_id, account_id, venue_id, adapter_instance_id
    ) ON DELETE RESTRICT,
    CONSTRAINT fk_orders_strategy_scope FOREIGN KEY (consumer_id, strategy_id)
        REFERENCES xuanling_tradecore_rm.strategies (consumer_id, strategy_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_orders_origin CHECK (origin IN ('PROGRAM', 'MANUAL_OMS')),
    CONSTRAINT ck_orders_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_orders_quantity CHECK (order_qty > 0),
    CONSTRAINT ck_orders_filled CHECK (filled_qty >= 0),
    CONSTRAINT ck_orders_leaves CHECK (leaves_qty >= 0),
    CONSTRAINT ck_orders_quantities CHECK (filled_qty + leaves_qty <= order_qty),
    CONSTRAINT ck_orders_average_fill_price CHECK (
        (filled_qty = 0 AND avg_fill_price IS NULL)
        OR (filled_qty > 0 AND avg_fill_price IS NOT NULL)
    ),
    CONSTRAINT ck_orders_risk_result CHECK (risk_result IN ('PASS', 'REJECTED', 'UNKNOWN')),
    CONSTRAINT ck_orders_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.order_executions (
    execution_id uuid NOT NULL,
    oms_order_id uuid NOT NULL,
    child_no integer NOT NULL,
    parent_execution_id uuid,
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    execution_qty numeric(24, 8) NOT NULL,
    execution_price numeric(24, 8),
    filled_qty numeric(24, 8) NOT NULL,
    execution_status varchar(48) NOT NULL,
    dispatch_phase varchar(48) NOT NULL,
    native_outcome varchar(48),
    adapter_client_scope_digest xuanling_tradecore_rm.sha256_hex NOT NULL,
    client_order_ref_kind varchar(32) NOT NULL,
    client_order_ref varchar(128) NOT NULL,
    client_order_ref_digest xuanling_tradecore_rm.sha256_hex NOT NULL,
    venue_order_id varchar(128),
    venue_order_id_digest xuanling_tradecore_rm.sha256_hex,
    broker_system_id varchar(64) NOT NULL,
    broker_strategy_id varchar(64) NOT NULL,
    broker_trader_id varchar(64) NOT NULL,
    broker_custom_ref varchar(256) NOT NULL,
    broker_custom_ref_chars smallint NOT NULL,
    broker_custom_ref_bytes smallint NOT NULL,
    broker_custom_ref_max_chars smallint NOT NULL,
    broker_custom_ref_max_bytes smallint NOT NULL,
    broker_custom_ref_encoding varchar(32) NOT NULL,
    broker_custom_ref_normalization_version integer NOT NULL,
    broker_custom_field_manifest_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_order_executions PRIMARY KEY (execution_id),
    CONSTRAINT uq_order_executions_order_execution UNIQUE (oms_order_id, execution_id),
    CONSTRAINT uq_order_executions_child UNIQUE (oms_order_id, child_no),
    CONSTRAINT uq_order_executions_client_route UNIQUE (
        venue_id, account_id, trading_day_epoch, adapter_client_scope_digest, client_order_ref_digest
    ),
    CONSTRAINT fk_order_executions_order_scope FOREIGN KEY (
        oms_order_id, account_id, venue_id, adapter_instance_id, trading_day, trading_day_epoch
    ) REFERENCES xuanling_tradecore_rm.orders (
        oms_order_id, account_id, venue_id, adapter_instance_id, trading_day, trading_day_epoch
    ) ON DELETE RESTRICT,
    CONSTRAINT fk_order_executions_parent FOREIGN KEY (oms_order_id, parent_execution_id)
        REFERENCES xuanling_tradecore_rm.order_executions (oms_order_id, execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_order_executions_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_order_executions_quantity CHECK (execution_qty > 0),
    CONSTRAINT ck_order_executions_filled CHECK (filled_qty >= 0 AND filled_qty <= execution_qty),
    CONSTRAINT ck_order_executions_child_no CHECK (child_no >= 0),
    CONSTRAINT ck_order_executions_system_id CHECK (broker_system_id ~ '^[A-Za-z0-9-]+$'),
    CONSTRAINT ck_order_executions_strategy_id CHECK (broker_strategy_id ~ '^[A-Za-z0-9-]+$'),
    CONSTRAINT ck_order_executions_trader_id CHECK (broker_trader_id ~ '^[A-Za-z0-9-]+$'),
    CONSTRAINT ck_order_executions_ref_composition CHECK (
        broker_custom_ref = broker_system_id || '_' || broker_strategy_id || '_' || broker_trader_id
    ),
    CONSTRAINT ck_order_executions_ref_char_count
        CHECK (broker_custom_ref_chars = char_length(broker_custom_ref) AND broker_custom_ref_chars > 0),
    CONSTRAINT ck_order_executions_ref_limits CHECK (
        broker_custom_ref_bytes > 0 AND broker_custom_ref_max_chars > 0
        AND broker_custom_ref_max_bytes > 0 AND broker_custom_ref_chars <= broker_custom_ref_max_chars
        AND broker_custom_ref_bytes <= broker_custom_ref_max_bytes
    ),
    CONSTRAINT ck_order_executions_ref_normalization CHECK (broker_custom_ref_normalization_version >= 1),
    CONSTRAINT ck_order_executions_revision CHECK (revision >= 1)
);

CREATE UNIQUE INDEX uq_order_executions_venue_order
    ON xuanling_tradecore_rm.order_executions (
        venue_id, account_id, trading_day_epoch, venue_order_id_digest
    )
    WHERE venue_order_id_digest IS NOT NULL;

CREATE TABLE xuanling_tradecore_rm.order_actions (
    action_id uuid NOT NULL,
    oms_order_id uuid NOT NULL,
    execution_id uuid,
    trading_day date NOT NULL,
    action_type varchar(32) NOT NULL,
    caller_action_key varchar(128) NOT NULL,
    action_status varchar(48) NOT NULL,
    dispatch_phase varchar(48) NOT NULL,
    reason_code varchar(64),
    operator_id uuid,
    venue_action_id varchar(128),
    error_code varchar(64),
    error_message varchar(512),
    requested_at timestamptz NOT NULL,
    completed_at timestamptz,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_order_actions PRIMARY KEY (action_id),
    CONSTRAINT uq_order_actions_caller_key UNIQUE (oms_order_id, action_type, caller_action_key),
    CONSTRAINT fk_order_actions_order FOREIGN KEY (oms_order_id)
        REFERENCES xuanling_tradecore_rm.orders (oms_order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_order_actions_execution FOREIGN KEY (oms_order_id, execution_id)
        REFERENCES xuanling_tradecore_rm.order_executions (oms_order_id, execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_order_actions_type CHECK (action_type IN ('CANCEL', 'MODIFY', 'ATTACH')),
    CONSTRAINT ck_order_actions_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.trades (
    trade_id uuid NOT NULL,
    oms_order_id uuid,
    execution_id uuid,
    consumer_id uuid,
    strategy_id uuid,
    trader_id uuid,
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    trade_key varchar(256) NOT NULL,
    venue_trade_id varchar(128),
    venue_trade_id_digest xuanling_tradecore_rm.sha256_hex,
    market varchar(32) NOT NULL,
    instrument_id varchar(64) NOT NULL,
    side varchar(16) NOT NULL,
    position_effect varchar(16),
    fill_qty numeric(24, 8) NOT NULL,
    fill_price numeric(24, 8) NOT NULL,
    fill_amount numeric(24, 8) NOT NULL,
    fee_amount numeric(24, 8),
    currency char(3) NOT NULL,
    trade_at timestamptz,
    received_at timestamptz NOT NULL,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_trades PRIMARY KEY (trade_id),
    CONSTRAINT uq_trades_trade_key UNIQUE (venue_id, account_id, trading_day_epoch, trade_key),
    CONSTRAINT fk_trades_account FOREIGN KEY (account_id, venue_id, adapter_instance_id)
        REFERENCES xuanling_tradecore_rm.accounts (account_id, venue_id, adapter_instance_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_trades_execution FOREIGN KEY (oms_order_id, execution_id)
        REFERENCES xuanling_tradecore_rm.order_executions (oms_order_id, execution_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_trades_attribution_pair CHECK (
        (oms_order_id IS NULL AND execution_id IS NULL)
        OR (oms_order_id IS NOT NULL AND execution_id IS NOT NULL)
    ),
    CONSTRAINT ck_trades_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_trades_quantity CHECK (fill_qty > 0),
    CONSTRAINT ck_trades_price CHECK (fill_price >= 0),
    CONSTRAINT ck_trades_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.assets (
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    snapshot_generation bigint NOT NULL,
    currency char(3) NOT NULL,
    total_asset numeric(24, 8) NOT NULL,
    net_asset numeric(24, 8) NOT NULL,
    cash_balance numeric(24, 8) NOT NULL,
    available_cash numeric(24, 8) NOT NULL,
    frozen_cash numeric(24, 8) NOT NULL,
    market_value numeric(24, 8) NOT NULL,
    margin_used numeric(24, 8) NOT NULL,
    total_debt numeric(24, 8) NOT NULL,
    credit_limit numeric(24, 8) NOT NULL,
    collateral_ratio numeric(24, 8),
    realized_pnl numeric(24, 8) NOT NULL,
    unrealized_pnl numeric(24, 8) NOT NULL,
    total_fee numeric(24, 8) NOT NULL,
    as_of_at timestamptz,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    updated_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_assets PRIMARY KEY (
        account_id, venue_id, adapter_instance_id, trading_day_epoch, currency
    ),
    CONSTRAINT fk_assets_account FOREIGN KEY (account_id, venue_id, adapter_instance_id)
        REFERENCES xuanling_tradecore_rm.accounts (account_id, venue_id, adapter_instance_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_assets_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_assets_generation CHECK (snapshot_generation >= 1),
    CONSTRAINT ck_assets_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.positions (
    account_id text NOT NULL,
    venue_id varchar(32) NOT NULL,
    adapter_instance_id text NOT NULL,
    trading_day date NOT NULL,
    trading_day_epoch bigint NOT NULL,
    snapshot_generation bigint NOT NULL,
    market varchar(32) NOT NULL,
    instrument_id varchar(64) NOT NULL,
    direction varchar(16) NOT NULL,
    total_qty numeric(24, 8) NOT NULL,
    available_qty numeric(24, 8) NOT NULL,
    frozen_qty numeric(24, 8) NOT NULL,
    today_qty numeric(24, 8) NOT NULL,
    yesterday_qty numeric(24, 8) NOT NULL,
    avg_open_price numeric(24, 8),
    cost_price numeric(24, 8),
    last_price numeric(24, 8),
    market_value numeric(24, 8) NOT NULL,
    realized_pnl numeric(24, 8) NOT NULL,
    unrealized_pnl numeric(24, 8) NOT NULL,
    as_of_at timestamptz,
    revision bigint NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    updated_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL,
    CONSTRAINT pk_positions PRIMARY KEY (
        account_id, venue_id, adapter_instance_id, trading_day_epoch, market, instrument_id, direction
    ),
    CONSTRAINT fk_positions_account FOREIGN KEY (account_id, venue_id, adapter_instance_id)
        REFERENCES xuanling_tradecore_rm.accounts (account_id, venue_id, adapter_instance_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_positions_direction CHECK (direction IN ('LONG', 'SHORT', 'NET')),
    CONSTRAINT ck_positions_epoch CHECK (trading_day_epoch >= 0),
    CONSTRAINT ck_positions_generation CHECK (snapshot_generation >= 1),
    CONSTRAINT ck_positions_total_qty CHECK (total_qty >= 0),
    CONSTRAINT ck_positions_available_qty CHECK (available_qty >= 0 AND available_qty <= total_qty),
    CONSTRAINT ck_positions_frozen_qty CHECK (frozen_qty >= 0 AND frozen_qty <= total_qty),
    CONSTRAINT ck_positions_revision CHECK (revision >= 1)
);

CREATE TABLE xuanling_tradecore_rm.projection_checkpoints (
    projector_name varchar(128) NOT NULL,
    shard_id varchar(64) NOT NULL,
    account_scope text NOT NULL,
    projector_version varchar(64) NOT NULL,
    journal_id uuid NOT NULL,
    stream_generation bigint NOT NULL,
    redis_ms bigint NOT NULL,
    redis_seq bigint NOT NULL,
    fact_ordinal integer NOT NULL,
    source_fact_hash xuanling_tradecore_rm.sha256_hex NOT NULL,
    status varchar(32) NOT NULL,
    lag_records bigint NOT NULL,
    lag_milliseconds bigint NOT NULL,
    last_error_code varchar(64),
    updated_at timestamptz NOT NULL,
    CONSTRAINT pk_projection_checkpoints PRIMARY KEY (projector_name, shard_id, account_scope),
    CONSTRAINT ck_projection_checkpoints_generation CHECK (stream_generation >= 0),
    CONSTRAINT ck_projection_checkpoints_position CHECK (
        redis_ms >= 0 AND redis_seq >= 0 AND fact_ordinal >= 0
    ),
    CONSTRAINT ck_projection_checkpoints_lag CHECK (lag_records >= 0 AND lag_milliseconds >= 0),
    CONSTRAINT ck_projection_checkpoints_status
        CHECK (status IN ('RUNNING', 'PAUSED', 'ERROR', 'REBUILDING'))
);

CREATE INDEX ix_accounts_consumer
    ON xuanling_tradecore_rm.accounts (consumer_id, lifecycle, account_display_name);
CREATE INDEX ix_orders_consumer_time
    ON xuanling_tradecore_rm.orders (consumer_id, created_at DESC, oms_order_id);
CREATE INDEX ix_orders_consumer_account_time
    ON xuanling_tradecore_rm.orders (consumer_id, account_id, created_at DESC, oms_order_id);
CREATE INDEX ix_orders_account_status
    ON xuanling_tradecore_rm.orders (account_id, trading_day, status, updated_at DESC);
CREATE INDEX ix_orders_strategy_time
    ON xuanling_tradecore_rm.orders (strategy_id, created_at DESC);
CREATE INDEX ix_orders_instrument_time
    ON xuanling_tradecore_rm.orders (market, instrument_id, created_at DESC);
CREATE INDEX ix_orders_working ON xuanling_tradecore_rm.orders (account_id, trading_day, updated_at DESC)
    WHERE status IN ('WORKING', 'PARTIALLY_FILLED', 'CANCEL_PENDING', 'SUBMIT_UNKNOWN');
CREATE INDEX ix_executions_order ON xuanling_tradecore_rm.order_executions (oms_order_id, child_no);
CREATE INDEX ix_executions_status
    ON xuanling_tradecore_rm.order_executions (account_id, trading_day, execution_status, updated_at DESC);
CREATE INDEX ix_executions_attribution
    ON xuanling_tradecore_rm.order_executions (broker_strategy_id, broker_trader_id, trading_day);
CREATE INDEX ix_actions_order_time
    ON xuanling_tradecore_rm.order_actions (oms_order_id, requested_at DESC);
CREATE INDEX ix_actions_status_time
    ON xuanling_tradecore_rm.order_actions (action_status, requested_at);
CREATE INDEX ix_trades_consumer_time
    ON xuanling_tradecore_rm.trades (consumer_id, trading_day, received_at DESC);
CREATE INDEX ix_trades_order_time
    ON xuanling_tradecore_rm.trades (oms_order_id, trade_at, received_at);
CREATE INDEX ix_trades_execution_time
    ON xuanling_tradecore_rm.trades (execution_id, received_at);
CREATE INDEX ix_trades_position_key
    ON xuanling_tradecore_rm.trades (account_id, trading_day, market, instrument_id);
CREATE INDEX ix_assets_account_day ON xuanling_tradecore_rm.assets (
    account_id, venue_id, adapter_instance_id, trading_day_epoch, currency
);
CREATE INDEX ix_positions_account ON xuanling_tradecore_rm.positions (
    account_id, venue_id, adapter_instance_id, trading_day_epoch, market, instrument_id
);
CREATE INDEX ix_positions_instrument
    ON xuanling_tradecore_rm.positions (instrument_id, market, trading_day_epoch);

ALTER TABLE xuanling_tradecore_rm.accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.accounts FORCE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.strategies ENABLE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.strategies FORCE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.orders FORCE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.trades FORCE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE xuanling_tradecore_rm.positions FORCE ROW LEVEL SECURITY;

CREATE POLICY accounts_consumer_read ON xuanling_tradecore_rm.accounts FOR SELECT TO tradecore_query
    USING (consumer_id = current_setting('app.consumer_id', true)::uuid);
CREATE POLICY strategies_consumer_read ON xuanling_tradecore_rm.strategies FOR SELECT TO tradecore_query
    USING (consumer_id = current_setting('app.consumer_id', true)::uuid);
CREATE POLICY orders_consumer_read ON xuanling_tradecore_rm.orders FOR SELECT TO tradecore_query
    USING (consumer_id = current_setting('app.consumer_id', true)::uuid);
CREATE POLICY trades_consumer_read ON xuanling_tradecore_rm.trades FOR SELECT TO tradecore_query
    USING (consumer_id = current_setting('app.consumer_id', true)::uuid);
CREATE POLICY positions_consumer_read ON xuanling_tradecore_rm.positions FOR SELECT TO tradecore_query
    USING (
        EXISTS (
            SELECT 1 FROM xuanling_tradecore_rm.accounts AS a
            WHERE a.account_id = positions.account_id
                AND a.venue_id = positions.venue_id
                AND a.adapter_instance_id = positions.adapter_instance_id
                AND a.trading_day_epoch = positions.trading_day_epoch
                AND a.consumer_id = current_setting('app.consumer_id', true)::uuid
        )
    );
CREATE POLICY accounts_projector_write ON xuanling_tradecore_rm.accounts FOR ALL TO tradecore_projector
    USING (true) WITH CHECK (true);
CREATE POLICY strategies_projector_write ON xuanling_tradecore_rm.strategies FOR ALL TO tradecore_projector
    USING (true) WITH CHECK (true);
CREATE POLICY orders_projector_write ON xuanling_tradecore_rm.orders FOR ALL TO tradecore_projector
    USING (true) WITH CHECK (true);
CREATE POLICY trades_projector_write ON xuanling_tradecore_rm.trades FOR ALL TO tradecore_projector
    USING (true) WITH CHECK (true);

CREATE VIEW xuanling_tradecore_rm.v_accounts
    WITH (security_barrier = true, security_invoker = true) AS
SELECT
    account_id, venue_id, adapter_instance_id, account_display_name, trading_day, trading_day_epoch,
    lifecycle, reconciliation_status, reconciliation_reason_code, active_gate_count, can_submit,
    submit_block_reason_code, revision, updated_at, projected_at
FROM xuanling_tradecore_rm.accounts;

CREATE VIEW xuanling_tradecore_rm.v_orders
    WITH (security_barrier = true, security_invoker = true) AS
SELECT
    oms_order_id, account_id, venue_id, adapter_instance_id, strategy_id, strategy_order_id, trader_id,
    origin, trading_day, trading_day_epoch, market, instrument_id, side, position_effect, order_type,
    time_in_force, currency, order_qty, limit_price, filled_qty, leaves_qty, avg_fill_price, status,
    risk_result, risk_reason_code, business_remark, revision, created_at, updated_at, terminal_at, projected_at
FROM xuanling_tradecore_rm.orders;

CREATE VIEW xuanling_tradecore_rm.v_active_orders
    WITH (security_barrier = true, security_invoker = true) AS
SELECT * FROM xuanling_tradecore_rm.v_orders
WHERE status IN ('WORKING', 'PARTIALLY_FILLED', 'CANCEL_PENDING', 'SUBMIT_UNKNOWN');

CREATE VIEW xuanling_tradecore_rm.v_trades
    WITH (security_barrier = true, security_invoker = true) AS
SELECT
    trade_id, oms_order_id, execution_id, account_id, venue_id, adapter_instance_id, trading_day,
    trading_day_epoch, market, instrument_id, side, position_effect, fill_qty, fill_price, fill_amount,
    fee_amount, currency, trade_at, received_at, revision, projected_at
FROM xuanling_tradecore_rm.trades;

CREATE VIEW xuanling_tradecore_rm.v_positions
    WITH (security_barrier = true, security_invoker = true) AS
SELECT
    p.account_id, p.venue_id, p.adapter_instance_id, p.trading_day, p.trading_day_epoch, p.market,
    p.instrument_id, p.direction, p.total_qty, p.available_qty, p.frozen_qty, p.today_qty,
    p.yesterday_qty, p.avg_open_price, p.cost_price, p.last_price, p.market_value, p.realized_pnl,
    p.unrealized_pnl, p.as_of_at, p.revision, p.updated_at, p.projected_at
FROM xuanling_tradecore_rm.positions AS p
JOIN xuanling_tradecore_rm.accounts AS a
    ON a.account_id = p.account_id
    AND a.venue_id = p.venue_id
    AND a.adapter_instance_id = p.adapter_instance_id
    AND a.trading_day_epoch = p.trading_day_epoch;

REVOKE ALL ON SCHEMA xuanling_tradecore_rm FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA xuanling_tradecore_rm FROM PUBLIC;
GRANT USAGE ON SCHEMA xuanling_tradecore_rm TO tradecore_projector, tradecore_query, tradecore_audit;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA xuanling_tradecore_rm TO tradecore_projector;
GRANT DELETE ON xuanling_tradecore_rm.assets, xuanling_tradecore_rm.positions TO tradecore_projector;
GRANT SELECT ON xuanling_tradecore_rm.v_accounts, xuanling_tradecore_rm.v_orders,
    xuanling_tradecore_rm.v_active_orders, xuanling_tradecore_rm.v_trades,
    xuanling_tradecore_rm.v_positions TO tradecore_query;
-- ``security_invoker`` views deliberately require these underlying relation
-- privileges.  RLS is forced on every granted table, including ``positions``;
-- direct SQL cannot widen the consumer boundary beyond the safe view result.
GRANT SELECT ON xuanling_tradecore_rm.accounts, xuanling_tradecore_rm.orders,
    xuanling_tradecore_rm.trades, xuanling_tradecore_rm.positions TO tradecore_query;
GRANT SELECT ON ALL TABLES IN SCHEMA xuanling_tradecore_rm TO tradecore_audit;
