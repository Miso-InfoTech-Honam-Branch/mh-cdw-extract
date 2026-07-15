# mh-cdw-extract

Python function-first extraction module with a thin FastAPI test wrapper.

## Install

```bash
cd /path/to/mh-cdw-extract
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
```

## Function Usage

```python
from cdw_extract import refresh_tables, preview, extract

refresh_tables("connection-001", request, data_root="/Users/root1/cdw")
rows = preview("connection-001", request, data_root="/Users/root1/cdw")
result = extract("connection-001", request, data_root="/Users/root1/cdw")
```

Refresh source support:

- `postgresql`, `mysql`, `mariadb`: DuckDB attach + Parquet copy
- `clickhouse`: ClickHouse HTTP `FORMAT Parquet`

Oracle refresh is intentionally not ported.

## Test API

```bash
.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8091
```

Supported test routes:

- `GET /health`
- `POST /api/v1/analytics/query`
- `POST /api/v1/user-datasets/{userDatasetId}/files/{userDatasetFileId}/convert`
- `DELETE /api/v1/user-datasets/{userId}/{userDatasetId}/files/{userDatasetFileId}`
- `POST /api/v1/connections/{connectionId}/tables/refresh`
- `POST /api/v1/connections/{connectionId}/tables/refresh-sync`
- `POST /api/v1/connections/{connectionId}/preview`
- `POST /api/v1/connections/{connectionId}/extracts`
- `POST /api/v1/connections/{connectionId}/delete`
- `GET /api/v1/jobs/{jobId}`
- `POST /api/v1/jobs/{jobId}/cancel`
- `GET /api/v1/jobs/{jobId}/download`

## Analytics query API

`POST /api/v1/analytics/query` is a synchronous, HTTP 200 internal API. It reads
only a published canonical `USER_DATST` Parquet artifact; upload and extract
results therefore use the same source contract. The request is a closed DSL:
raw SQL, functions, and expressions are not accepted.

```json
{
  "schemaVersion": 1,
  "requestId": "chart-request-1",
  "source": {
    "sourceKind": "USER_DATST",
    "userId": "user-1",
    "userDatasetId": "dataset-1",
    "userDatasetFileId": "file-1"
  },
  "chartType": "BAR",
  "encoding": {
    "category": {"column": "department", "label": "Department"},
    "value": {"column": "amount", "aggregation": "SUM", "label": "Amount"}
  },
  "filters": [{"column": "status", "operator": "EQ", "value": "ACTIVE"}],
  "sorts": [{"field": "value", "direction": "DESC"}],
  "limit": 20,
  "options": {"nullPolicy": "EXCLUDE", "includeOthers": true, "othersLabel": "Others"}
}
```

Supported charts are `BAR`, `PIE`, `LINE`, `SCATTER`, `BOXPLOT`, `FUNNEL`,
`SANKEY`, and `TREEMAP`. Aggregations are `COUNT`, `COUNT_DISTINCT`, `SUM`,
`AVG`, `MIN`, `MAX`, and `MEDIAN`; time grains are `DAY`, `WEEK`, `MONTH`,
`QUARTER`, and `YEAR`. Filters support `EQ`, `NE`, `GT`, `GTE`, `LT`, `LTE`,
`IN`, `CONTAINS`, `BETWEEN`, `IS_NULL`, and `IS_NOT_NULL`.

| chartType | Required/optional encoding roles | Row keys |
| --- | --- | --- |
| `BAR`, `PIE`, `LINE` | `category`; optional `value`, `series` | `category`, `value`, optional `series` |
| `SCATTER` | `x`, `y`; optional `size`, `series` | `x`, `y`, optional `size`, `series` |
| `BOXPLOT` | `value`; optional `group` (defaults to `All`) | `category`, `count`, `min`, `q1`, `median`, `q3`, `max`, `lowerFence`, `upperFence`, `outlierCount` |
| `FUNNEL` | one of `stage`/`category`; optional `value`, `series` | `category`, `value`, optional `series` |
| `SANKEY` | `source`, `target`; optional `value` | `source`, `target`, `value` |
| `TREEMAP` | `hierarchy` with 1-3 fields; optional `value` | `level0`...`levelN`, `value` |

For grouped charts, an omitted `value` means `COUNT(*)`; a value column with no
aggregation defaults to `SUM`. `COUNT` may omit its column. `timeGrain` is valid
only on `DATE`/`TIMESTAMP` dimensions. Sankey node IDs are normalized to strings,
and self-links are excluded by default.

Typed options include `nullPolicy`, `includeOthers`, `othersLabel`,
`excludeSelfLinks`, `scatterSampleSize`, `randomSeed`, `memoryLimitMb`, `threads`,
and `timeoutMs`. `includeOthers` is limited to `BAR`/`PIE`/`FUNNEL` with additive
`COUNT`/`SUM`, no series, and `limit >= 2`; a label that collides with real data
is rejected. Presentation-only options are not part of the worker DSL and must
be removed by the caller.

```json
{
  "requestId": "chart-request-1",
  "chartType": "BAR",
  "sourceVersion": "sha256:...",
  "elapsedMs": 12,
  "rowCount": 2,
  "truncated": false,
  "warnings": [],
  "columns": [
    {"key": "category", "label": "Department", "type": "STRING"},
    {"key": "value", "label": "Amount", "type": "NUMBER"}
  ],
  "rows": [
    {"category": "Cardiology", "value": 1200},
    {"category": "Neurology", "value": 900}
  ]
}
```

The service re-reads the Parquet schema, validates every referenced column and
role, quotes identifiers, and binds filter values. Defaults cap source files at
256 MiB, results at 2,000 rows, DuckDB memory at 256 MiB, execution at 5 seconds,
and scatter reservoir samples at 1,000 points. `ANALYTICS_MAX_SOURCE_BYTES` can
set the deployment source-file cap. Any omitted result rows set `truncated=true`
and add a warning; dates are ISO strings and non-finite numbers become JSON null
with a warning.

Invalid request shapes/enums return FastAPI `422` validation details. A valid
shape that violates chart roles, source columns, or type compatibility returns
`400`; missing published artifacts return `404`, and an interrupted timeout
returns `408`.

`POST /api/v1/connections/{connectionId}/extracts` returns HTTP `202 Accepted`
after persisting the job, then performs the export in a background task. The job
state can be read from `GET /api/v1/jobs/{jobId}` and transitions through
`ACCEPTED -> RUNNING -> COMPLETED|FAILED|CANCELLED` or
`ACCEPTED -> CANCELLED`. A running extract is reported as cancelled only after
its active DuckDB connection has been interrupted and the runner has persisted
the terminal `CANCELLED` state.

## Worker runtime limits

All DuckDB callers share a bounded operation pool and each connection receives
its own spill directory beneath `${DATA_ROOT}/_tmp/duckdb`, which is removed on
close. Runtime settings are:

- `DUCKDB_MAX_CONCURRENT_OPERATIONS` (default `4`): process-wide DuckDB limit
- `DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS` (default `60`): wait for a slot
- `DUCKDB_TEMP_ROOT`: fallback spill root when a caller has no `DATA_ROOT`
- `EXTRACT_CANCEL_WAIT_SECONDS` (default `2`): how long the cancel API waits for
  a running extract to confirm terminal cancellation

Running-job cancellation uses an in-process connection registry. Run uvicorn
with one worker when confirmed running cancellation is required. If a cancel
request reaches a different worker process, the API keeps the current state and
returns `cancelSupported=false` instead of claiming that the job stopped. Job
manifest writes themselves use per-job thread and OS file locks plus unique
atomic temp files, so concurrent writers do not share `job.json.tmp`.

To publish a completed Parquet export as a reusable MyHome table, Boot includes
the extract correlation and a reserved result target:

```json
{
  "datasetId": "extract-dataset-id",
  "runId": "extract-run-id",
  "outputFormat": "parquet",
  "resultTarget": {
    "kind": "USER_DATST",
    "userId": "requesting-user-id",
    "userDatasetId": "reserved-user-dataset-id",
    "userDatasetFileId": "reserved-user-dataset-file-id",
    "idempotencyKey": "EXPORT:extract-run-id"
  },
  "callback": {
    "url": "http://boot/api/v1/extract-worker/datasets/extract-dataset-id/status"
  }
}
```

The worker atomically publishes `parquet/data.parquet`, `meta/manifest.json`,
and `meta/schema.json` beneath the reserved user-dataset file directory. The
terminal callback and job status both retain the result IDs and the schema read
from the completed Parquet file, so Boot can recover through polling if callback
delivery fails. A result target is intentionally limited to Parquet output.

`/api/v1/user-datasets/{userDatasetId}/files/{userDatasetFileId}/convert` is the
Boot-facing user file conversion route. It accepts multipart fields:

- `file`: original CSV/XLSX/Parquet upload
- `request`: JSON object with Boot-issued `jobId`, `requestId`, `userId`, `originalFileName`, `fileType`,
  `options`, and optional `callback`

It returns immediately with:

```json
{
  "requestId": "request-id",
  "jobId": "same-boot-issued-job-id",
  "jobType": "USER_DATASET_CONVERT",
  "state": "ACCEPTED"
}
```

Converted user files are stored under:

```text
${DATA_ROOT}/user-datasets/{userId}/{userDatasetId}/files/{userDatasetFileId}/parquet/data.parquet
```

Refreshed DB source tables are stored under:

```text
${DATA_ROOT}/connections/{connectionId}/tables/*.parquet
${DATA_ROOT}/connections/{connectionId}/manifest.json
```

Extraction and preview join requests can reference user files with:

```json
{
  "sourceKind": "USER_DATST",
  "userId": "user-1",
  "userDatasetId": "uds-1",
  "userDatasetFileId": "udf-1",
  "alias": "upload"
}
```

## Analytics interaction DSL and MyHome artifacts

The v1 analytics query remains backward compatible and accepts additive global,
chart, and click-interaction filters. All four arrays (`filters`,
`globalFilters`, `chartFilters`, and `interactionFilters`) are combined with
`AND`. A temporal click bucket may carry `timeGrain`; the worker applies the
same `date_trunc` expression used by the chart instead of comparing the raw date.
Numeric bins are deterministic fixed-width bins. `bin.size` must be a positive
finite number; automatic bin selection is intentionally a UI/statistics step so
that a saved analysis does not silently change its boundaries.

```json
{
  "schemaVersion": 1,
  "requestId": "chart-42",
  "source": {
    "sourceKind": "USER_DATST",
    "userId": "user-1",
    "userDatasetId": "dataset-1",
    "userDatasetFileId": "file-1"
  },
  "chartType": "LINE",
  "calculatedFields": [
    {
      "id": "net_amount",
      "name": "Net amount",
      "dataType": "NUMBER",
      "formula": {
        "op": "SUBTRACT",
        "args": [
          {"op": "COLUMN", "column": "amount"},
          {"op": "COALESCE", "args": [
            {"op": "COLUMN", "column": "discount"},
            {"op": "LITERAL", "value": 0}
          ]}
        ]
      }
    }
  ],
  "encoding": {
    "category": {"column": "visit_date", "timeGrain": "MONTH"},
    "value": {"derivedFieldId": "net_amount", "aggregation": "SUM"}
  },
  "globalFilters": [{"column": "hospital", "operator": "EQ", "value": "MAIN"}],
  "chartFilters": [{"column": "amount", "operator": "GTE", "value": 0}],
  "interactionFilters": [
    {"column": "visit_date", "timeGrain": "MONTH", "operator": "EQ", "value": "2026-07-01"}
  ],
  "topN": {"enabled": true, "count": 10, "by": "value", "direction": "DESC", "includeOthers": false},
  "drilldown": {
    "fields": [{"column": "hospital"}, {"column": "department"}],
    "level": 0
  },
  "comparison": {
    "enabled": true,
    "mode": "PREVIOUS_PERIOD",
    "periodUnit": "MONTH",
    "offset": -1
  },
  "referenceLines": [
    {"id": "mean", "type": "AVERAGE", "label": "Average"},
    {"id": "goal", "type": "TARGET", "value": 1000, "label": "Goal", "color": "#dc2626"}
  ],
  "sorts": [{"field": "category", "direction": "ASC"}],
  "limit": 100,
  "options": {"valueTransform": "RUNNING_TOTAL"}
}
```

`topN.by` is deliberately closed to `value`. `valueTransform` is `NONE`,
`PERCENT_OF_TOTAL`, or `RUNNING_TOTAL`. A `SERIES` comparison supplies a
dimension in `comparison.field`; a `PREVIOUS_PERIOD` comparison requires the
same `periodUnit` as the active category `timeGrain` and returns
`previousValue`, `change`, and `changeRate`. It uses the previous available
bucket and emits a warning when calendar gaps are not synthesized.

Calculated fields never accept formula/SQL strings. Their closed expression
tree supports `COLUMN`, `LITERAL`, `ADD`, `SUBTRACT`, `MULTIPLY`, `DIVIDE`,
`COALESCE`, `CONCAT`, `DATE_DIFF`, `DATE_PART`, comparisons, boolean
`AND`/`OR`/`NOT`, and `CASE`. Identifiers are schema-checked and quoted; literal
values are bound parameters. Formula depth is capped at 8 and total nodes at
100. Encodings and filters reference a calculated field with
`derivedFieldId` (`calculatedFieldId` is accepted as an input compatibility
alias).

Raw rows for drilldown/detail modals use `POST /api/v1/analytics/detail`:

```json
{
  "schemaVersion": 1,
  "requestId": "detail-42",
  "source": {
    "sourceKind": "USER_DATST",
    "userId": "user-1",
    "userDatasetId": "dataset-1",
    "userDatasetFileId": "file-1"
  },
  "calculatedFields": [],
  "globalFilters": [],
  "chartFilters": [],
  "interactionFilters": [],
  "detailColumns": [{"column": "patient_id"}, {"column": "visit_date"}],
  "sorts": [{"field": "visit_date", "direction": "DESC"}],
  "offset": 0,
  "limit": 200
}
```

The response contains `offset`, `limit`, `rowCount`, `hasMore`, `columns`, and
`rows`, and uses the same source, calculation, filter, timeout, and memory
validation as chart queries.

Boot creates a personalized file with `POST /api/v1/analytics/artifacts`. Boot
pre-issues the UUID job ID and converts saved UI column IDs to physical,
whitelisted worker query fields. The raw dashboard snapshot may be retained in
`dashboard`, but only the strict `queries[].query` objects are executed.

```json
{
  "schemaVersion": 1,
  "jobId": "a89dfc80-51cc-4b3c-89b4-c4e90adf1bb2",
  "requestId": "artifact-request-1",
  "artifactId": "artifact-1",
  "analysisId": "analysis-1",
  "userId": "user-1",
  "name": "July dashboard",
  "format": "PDF",
  "spec": {
    "specVersion": 2,
    "title": "July dashboard",
    "dashboard": {},
    "queries": [
      {
        "chartId": "chart-1",
        "title": "Monthly patients",
        "query": {
          "schemaVersion": 1,
          "requestId": "render-chart-1",
          "source": {
            "sourceKind": "USER_DATST",
            "userId": "user-1",
            "userDatasetId": "dataset-1",
            "userDatasetFileId": "file-1"
          },
          "chartType": "LINE",
          "encoding": {
            "category": {"column": "visit_date", "timeGrain": "MONTH"},
            "value": {"aggregation": "COUNT"}
          },
          "filters": [],
          "sorts": [{"field": "category", "direction": "ASC"}],
          "limit": 100,
          "options": {}
        },
        "layout": {"chartId": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}
      }
    ]
  },
  "callback": {
    "url": "http://boot/api/v1/extract-worker/analysis-artifacts/artifact-1/status",
    "timeoutSeconds": 10
  }
}
```

The accepted response is:

```json
{
  "jobId": "a89dfc80-51cc-4b3c-89b4-c4e90adf1bb2",
  "jobType": "ANALYSIS_ARTIFACT",
  "requestId": "artifact-request-1",
  "artifactId": "artifact-1",
  "analysisId": "analysis-1",
  "userId": "user-1",
  "state": "ACCEPTED"
}
```

PNG and PDF render every dashboard panel with Matplotlib's headless backend;
XLSX contains the dashboard image and one data sheet per chart. The renderer
supports all eight chart types, discovers Malgun Gothic/Noto Sans CJK/Nanum
fonts, records a manifest warning if no CJK font is available, and replaces an
individual broken panel with an error panel without corrupting other charts.
For each compiled query, the renderer matches `queries[].chartId` to the raw
`dashboard.charts[].chartId` and applies the saved display options to every
format: `palette` (`PROFESSIONAL`, `OCEAN`, `WARM`, or `ACCESSIBLE`),
`numberFormat` (`AUTO`, `NUMBER`, `COMPACT`, `PERCENT`, or `CURRENCY_KRW`),
`decimalPlaces` (0-3), `showGrid`, and `axisLabelRotation` (`AUTO`, 0, 30, or
45). Missing or invalid display values fall back to the frontend defaults.

Artifacts are built below `_staging/{jobId}` using `.part` files, then the file
and READY manifest directory are atomically renamed to:

```text
${DATA_ROOT}/analysis-artifacts/{userId}/{artifactId}/files/{name}.{png|pdf|xlsx}
${DATA_ROOT}/analysis-artifacts/{userId}/{artifactId}/meta/manifest.json
```

The terminal callback is retried up to three times and includes `jobId`, all
three identities, `status`, `fileName`, `relativePath`, `contentType`,
`sizeBytes`, `checksumSha256`, `sourceVersion`, `errorCode`, and `message`. The same metadata is
kept in `GET /api/v1/jobs/{jobId}` so Boot can reconcile a lost callback.

Headless Matplotlib rendering is serialized by default because its global font
and figure state is not thread-safe. Set `ANALYTICS_ARTIFACT_MAX_CONCURRENT` to
a positive integer only after measuring the worker's memory and renderer
stability. A request waits up to
`ANALYTICS_ARTIFACT_QUEUE_TIMEOUT_SECONDS` (default 60 seconds) for a render
slot, checking cancellation while queued; a timeout finishes the job as
`FAILED` and is reported through the normal terminal callback.

Worker storage does not need to be shared with Boot. Boot proxies:

```text
GET    /api/v1/analytics/artifacts/{userId}/{artifactId}/download
DELETE /api/v1/analytics/artifacts/{userId}/{artifactId}
```

Download verifies READY state, identity, canonical path, byte size, and SHA-256
before streaming. Delete first persists a tombstone and signals the active job,
then removes staging/final files; the publisher rechecks the tombstone while
holding the same identity lock, so a late render cannot resurrect a deleted
MyHome artifact.
