"""Isolated EMS and strategy-consumer performance contracts.

The package is deliberately separate from the EMT execution process.  It
exercises the real EMS planner/routing and ConsumerGateway authorization
contracts with deterministic, payload-free inputs, but never imports an EMT
SDK or performs a broker mutation.
"""
