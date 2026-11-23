-- Explicit F3 rollback for the initial non-authoritative read model.
-- This is a deployment operation only.  It never runs from an OMS, EMS,
-- Adapter, callback or application-start path.

REVOKE ALL ON SCHEMA xuanling_tradecore_rm FROM tradecore_projector, tradecore_query, tradecore_audit;
DROP SCHEMA IF EXISTS xuanling_tradecore_rm CASCADE;
