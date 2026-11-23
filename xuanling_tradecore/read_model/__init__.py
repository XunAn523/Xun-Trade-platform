"""F3's non-authoritative PostgreSQL read-model migration boundary.

The package intentionally exposes migration discovery and an explicit operator
entry point only.  Importing an OMS, EMS or Adapter component never opens a
database connection and never performs DDL.
"""

from .journal_projector_v1 import (
    BoundedReadModelProjectorQueueV1,
    CommittedReadModelFactMapperV1,
    CommittedReadModelFactV1,
    JournalFirstProjectionErrorV1,
    JournalFirstReadModelProjectorV1,
    ReadModelProjectorHealthV1,
)
from .migration_catalog import (
    ReadModelMigrationError,
    ReadModelMigrationV1,
    apply_read_model_migrations,
    load_read_model_migrations,
    read_model_schema_hash,
    rollback_read_model_migrations,
)
from .postgres_projector_v1 import (
    PostgresReadModelProjectionErrorV1,
    PostgresReadModelProjectionStoreV1,
    ReadModelProjectionCommitV1,
)
from .projection_v1 import (
    InMemoryReadModelProjectionStoreV1,
    ReadModelCheckpointKeyV1,
    ReadModelCheckpointV1,
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
    ReadModelProjectionSnapshotV1,
    ReadModelRowMutationV1,
    ReadModelSnapshotReplacementV1,
    ReadModelTableV1,
)
from .query_service_v1 import (
    PostgresReadModelQueryServiceV1,
    ReadModelConsumerScopeV1,
    ReadModelOrderCursorV1,
    ReadModelOrderPageV1,
    ReadModelOrderRowV1,
    ReadModelQueryErrorV1,
    ReadModelWatermarkV1,
)

__all__ = [
    "ReadModelMigrationError",
    "ReadModelMigrationV1",
    "apply_read_model_migrations",
    "load_read_model_migrations",
    "read_model_schema_hash",
    "rollback_read_model_migrations",
    "InMemoryReadModelProjectionStoreV1",
    "BoundedReadModelProjectorQueueV1",
    "CommittedReadModelFactMapperV1",
    "CommittedReadModelFactV1",
    "JournalFirstProjectionErrorV1",
    "JournalFirstReadModelProjectorV1",
    "PostgresReadModelProjectionErrorV1",
    "PostgresReadModelProjectionStoreV1",
    "ReadModelCheckpointKeyV1",
    "ReadModelCheckpointV1",
    "ReadModelPositionV1",
    "ReadModelProjectionBatchV1",
    "ReadModelProjectionCommitV1",
    "ReadModelProjectionErrorV1",
    "ReadModelProjectorHealthV1",
    "ReadModelProjectionSnapshotV1",
    "ReadModelRowMutationV1",
    "ReadModelSnapshotReplacementV1",
    "ReadModelTableV1",
    "PostgresReadModelQueryServiceV1",
    "ReadModelConsumerScopeV1",
    "ReadModelOrderCursorV1",
    "ReadModelOrderPageV1",
    "ReadModelOrderRowV1",
    "ReadModelQueryErrorV1",
    "ReadModelWatermarkV1",
]
