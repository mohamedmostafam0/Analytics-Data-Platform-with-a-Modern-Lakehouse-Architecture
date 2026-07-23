# Kubernetes feature matrix

## Phase 3A behavior

All component flags default to false, so base values remain namespace-only. The
`minimal` overlay opts into the approved `sourcePostgresql` and
`itemsLoadGenerator` Phase 2 slice. The `batch` overlay opts into the main
PostgreSQL, MinIO, and finite private-bucket bootstrap slice in a separate release.
All later features remain locked by the Phase 3A implementation gate.

| Feature | Default | Target profiles | Dependencies enforced | Conflicts/advisories | Config | Validation | Phase |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `postgresql` | off | batch, analytics | external Secret | singleton stateful development database | `features.postgresql.enabled` | render + restore/hibernate smoke | 3A |
| `sourcePostgresql` | off | minimal | external Secret | singleton development database | `features.sourcePostgresql.enabled` | rendered + runtime smoke | 2 |
| `cdcPostgresql` | off | ingestion | none | stateful/WAL source | `features.cdcPostgresql.enabled` | phase gate | 3 |
| `itemsLoadGenerator` | off | minimal | `sourcePostgresql` | finite; skip existing by default | `features.itemsLoadGenerator.enabled` | dependency + unit/render/runtime smoke | 2 |
| `systemLoadGenerator` | off | batch | `postgresql`, `minio` | high data volume | `features.systemLoadGenerator.enabled` | dependency + phase gate | 4 |
| `flashSaleLoadGenerator` | off | ingestion | `cdcPostgresql` | continuous writer | `features.flashSaleLoadGenerator.enabled` | dependency + phase gate | 3 |
| `minio` | off | batch, analytics | external Secret | archived upstream; local compatibility only | `features.minio.enabled` | render + restart/hibernate persistence smoke | 3A |
| `minioClient` | off | batch, analytics | `minio` | finite private-bucket Job | `features.minioClient.enabled` | dependency + private/idempotent runtime smoke | 3A |
| `icebergRest` | off | batch, analytics | `minio` | Polaris selected; deployment deferred | `features.icebergRest.enabled` | dependency + phase gate | 3A decision |
| `spark` | off | batch | `minio`, `icebergRest` | very large | `features.spark.enabled` | dependency + phase gate | 4 |
| `trino` | off | analytics | `minio`, `icebergRest` | large | `features.trino.enabled` | dependency + phase gate | 5 |
| `kafka` | off | ingestion, streaming | none | stateful/large | `features.kafka.enabled` | phase gate | 3 |
| `kafkaSetup` | off | streaming | `kafka` | setup Job | `features.kafkaSetup.enabled` | dependency + phase gate | 3 |
| `schemaRegistry` | off | ingestion, streaming | `kafka` | JVM service | `features.schemaRegistry.enabled` | dependency + phase gate | 3 |
| `kafkaConnect` | off | ingestion | `kafka`, `schemaRegistry` | plugin supply chain | `features.kafkaConnect.enabled` | dependency + phase gate | 3 |
| `connectorSetup` | off | ingestion | `kafkaConnect`, `cdcPostgresql`, `openSearch` | expands secrets | `features.connectorSetup.enabled` | dependency + phase gate | 3 |
| `loginLoadGenerator` | off | streaming | `kafka`, `schemaRegistry` | continuous writer | `features.loginLoadGenerator.enabled` | dependency + phase gate | 3 |
| `redpandaConsole` | off | ingestion, streaming | `kafka`, `schemaRegistry` | support UI | `features.redpandaConsole.enabled` | dependency + phase gate | 3 |
| `flink` | off | streaming | `kafka`, `schemaRegistry` | large/stateful jobs | `features.flink.enabled` | dependency + phase gate | 4 |
| `clickhouse` | off | analytics | `kafka`, `schemaRegistry` | stateful/large | `features.clickhouse.enabled` | dependency + phase gate | 5 |
| `openSearch` | off | ingestion, logging | none | stateful/large | `features.openSearch.enabled` | phase gate | 5/7 |
| `streamlit` | off | analytics | `clickhouse`, `postgresql` | large ML image | `features.streamlit.enabled` | dependency + phase gate | 5 |
| `superset` | off | analytics | `trino` | includes metadata DB | `features.superset.enabled` | dependency + phase gate | 5 |
| `airflow` | off | batch | none to start; DAG integrations documented | multiple services | `features.airflow.enabled` | phase gate | 4 |
| `mailhog` | off | batch | none | development only | `features.mailhog.enabled` | phase gate | 4 |

Every feature is also a member of target profile `full`, which is deliberately
unsuitable for a laptop. No repository-supported hard conflict was found; resource
advisories are enforced operationally rather than as Helm failures.
