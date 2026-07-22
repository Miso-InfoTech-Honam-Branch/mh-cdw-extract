# 큐 작업 프로젝트에 넣는 방법 (v2)

`mh-cdw-extract`의 새 진입점은 `CdwEngine`이다. 이 경계 안에는 HTTP 서버, Boot callback, 큐 상태 저장 코드가 없다. 큐 프로젝트는 작업 수신·재시도·상태 저장·callback 전송을 담당하고, 이 모듈은 검증된 작업을 실행해 `JobResult`를 돌려준다.

## 작업 흐름

1. Boot가 큐 API에 `JobEnvelope`를 보낸다.
2. 큐는 envelope과 attempt, 마지막 callback sequence를 영속 저장한다.
3. 큐 worker가 `CdwEngine.execute()`를 호출한다.
4. 결과를 `JobCallbackEvent.from_result()`로 변환하고 `transport_dict()`로 직렬화한다.
5. 큐가 Boot의 `/api/internal/v2/job-callbacks`로 전송한다.
6. callback이 확인된 뒤 큐가 작업을 완료 처리한다.

```python
from cdw_extract import (
    CdwEngine,
    ExecutionContext,
    JobCallbackEvent,
    JobEnvelope,
)
from cdw_extract.adapters import legacy_runtime_services

services = legacy_runtime_services("/srv/cdw/data")
engine = CdwEngine(services)

envelope = JobEnvelope.model_validate(message_body)
result = engine.execute(
    envelope,
    ExecutionContext(
        job_id=envelope.job_id,
        attempt=queue_record.attempt,
        event_sequence_start=queue_record.last_callback_sequence,
    ),
)

callback = JobCallbackEvent.from_result(
    result,
    sequence=queue_record.next_callback_sequence(),
    attempt=queue_record.attempt,
    queue_job_id=queue_record.id,
)
boot_client.post_callback(callback.transport_dict())
```

`JobResult`를 그대로 callback으로 보내면 안 된다. Boot callback에는 `eventId`, 작업 전체에서 단조 증가하는 `sequence`, `attempt`가 추가로 필요하다. 같은 sequence를 다시 변환하면 같은 `eventId`가 생성되므로 안전하게 재전송할 수 있다.

## 취소

Boot의 취소 body는 `CancellationEnvelope`로 검증한다. 실행 중인 작업과 연결하거나, 실행 전에 도착한 취소를 tombstone으로 보관하려면 engine이 사용하는 registry에 전달한다.

```python
from cdw_extract import CancellationEnvelope

cancel = CancellationEnvelope.model_validate(message_body)
services.cancellations.cancel(cancel)
```

registry는 같은 Python 프로세스 안에서 DuckDB `interrupt()`까지 전달한다. 큐 프로젝트는 취소 tombstone을 DB에도 저장해야 한다. terminal 결과가 Boot에 확인된 뒤 `services.cancellations.forget(job_id)`를 호출한다.

## 16 CPU / 32 GiB 기준 자원값

기본 동시 DuckDB 작업 수는 4다. 각 envelope의 `resourceBudget`을 적용하되, 아래 프로세스 전체 한도를 넘는 요청은 자동으로 줄이고 FIFO로 대기시킨다.

```dotenv
DUCKDB_MAX_CONCURRENT_OPERATIONS=4
DUCKDB_TOTAL_THREADS=16
DUCKDB_TOTAL_MEMORY_BYTES=25769803776
DUCKDB_TOTAL_TEMP_BYTES=68719476736
DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS=60
```

권장 일반 추출 1건의 budget은 `cpuThreads=4`, `memoryBytes=4294967296`(4 GiB)다. 큰 GROUP/PIVOT/JOIN 작업은 8 thread·8 GiB로 올리면 다른 작업은 weighted gate에서 기다린다. 따라서 무조건 4건을 동시에 메모리에 밀어 넣지 않으면서, 작은 작업은 병렬 처리할 수 있다.

`outputBytes`와 `rowLimit`이 있으면 engine은 SUCCESS를 만들기 전에 반환된 artifact/metric의 실제 크기와 행 수를 다시 검사한다. 초과 결과는 `RESOURCE_LIMIT_EXCEEDED` 실패로 바뀌므로 큐는 성공 callback을 보내면 안 된다.

ClickHouse 메타데이터 Parquet는 확장 가능한 영구 스냅샷 볼륨으로 직접 스트리밍하므로
`tempBytes`를 결과 파일 크기 제한으로 사용하지 않는다. 운영자가 `outputBytes`를 명시한 경우에만
전체 테이블 결과 합계에 그 제한을 적용하며, 취소 시 스트림 종료와 부분 파일 삭제는 항상 유지한다.

고급 추출 검증 응답은 자동 PIVOT 값을 모두 채운 전체 `resolvedPipeline`을 반환한다. 함께 반환하는 `pipelineHash`는 반드시 그 `resolvedPipeline`을 canonical JSON으로 직렬화한 SHA-256이다. Boot는 원래 요청이 아니라 이 두 값을 저장하고, 실행 command의 `pipeline`과 `expectedPipelineHash`로 그대로 전달해야 한다. 이미 값이 지정된 fixed PIVOT은 수정 없이 동일한 스냅샷과 해시를 반환한다.

Boot가 저장한 고급 추출 스냅샷에는 `expectedCompilerVersion`도 포함된다. worker의 compiler가 다르면 실행을 계속하지 않고 `PIPELINE_COMPILER_VERSION_MISMATCH`로 실패하므로, 배포로 인해 과거 파이프라인의 의미가 조용히 바뀌지 않는다.

`DUCKDB_TOTAL_TEMP_BYTES`는 시작 시 실제 임시 디스크 여유 공간의 80%를 넘지 않게 다시 제한된다. 큐 worker를 여러 프로세스로 띄우면 이 합산 gate도 프로세스별로 생기므로, 프로세스별 환경값을 나눠 주거나 큐의 worker concurrency를 함께 제한해야 한다.

## 아티팩트 규칙

- key는 저장소 기준 상대 경로만 허용한다.
- `sha256`, `sizeBytes`, `contentType`, `format`은 필수다.
- 같은 idempotency key의 결과는 기존 파일을 덮어쓰지 않는다.
- SUCCESS callback에만 artifacts를 넣는다.
- Python model은 Boot DB 길이와 enum을 동일하게 검증한다.

큐 프로젝트는 `transport_dict()` 또는 `transport_json()`만 사용해야 한다. 일반 `model_dump()`는 Python의 snake_case 필드명을 만들 수 있다.
