# Queue host v2 체크리스트

이 패키지는 queue 서버가 아니라 실행 모듈이다. Queue 프로젝트는 아래 항목을 구현해야 한다.

- `JobEnvelope`와 `CancellationEnvelope`를 `transport_dict()` 형식 그대로 수신한다.
- job attempt와 마지막 callback sequence를 DB에 저장한다.
- `CdwEngine.execute()`의 `JobResult`를 `JobCallbackEvent.from_result()`로 변환한다.
- Boot가 callback을 확인하기 전에는 queue 메시지를 완료 처리하지 않는다.
- 같은 sequence의 재전송에는 같은 deterministic `eventId`를 사용한다.
- 작업 종료 callback 확인 뒤 `services.cancellations.forget(job_id)`를 호출한다.

Metadata refresh는 `connections/{connectionId}/tables/{snapshotId}` 아래에 불변 스냅샷을 게시한 뒤 manifest를 원자적으로 전환한다. 실행 중인 추출이 참조하는 이전 스냅샷은 삭제하지 말고, queue host의 보존 작업이 Boot의 활성 job/data-asset 참조를 확인한 뒤 정리해야 한다.

`JobCallbackEvent.metrics`는 기존 업무 상태 투영에 사용된다. JSON object만 허용하며 UTF-8 직렬화 크기가 2 MiB를 넘으면 안 된다. 주요 값은 `rowCount`, `resultColumns`, metadata refresh의 `tables`, 분석 artifact 식별자다. 절대 파일 경로와 비밀값은 metrics에 넣지 않는다.

## 입력과 비밀값

`DATASET_CONVERT`는 `command.input`의 `ArtifactDescriptor`를 `ArtifactStore.materialize()`로 해석한다. Host adapter는 materialize한 파일의 checksum, size, 선택적 version을 검증해야 한다.

`METADATA_REFRESH`는 inline username/password를 거절한다. Host는 `command.secretRef`를 `SecretProvider.resolve(reference, purpose="METADATA_REFRESH")`로 해석하여 실행 중에만 자격 증명을 빌려준다. 자격 증명을 envelope, queue DB, log, callback에 저장하지 않는다.

## 16 CPU / 32 GiB host

권장 시작값:

```dotenv
DUCKDB_MAX_CONCURRENT_OPERATIONS=4
DUCKDB_TOTAL_THREADS=16
DUCKDB_TOTAL_MEMORY_BYTES=25769803776
DUCKDB_TOTAL_TEMP_BYTES=68719476736
DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS=60
```

이 aggregate gate는 Python 프로세스 내부 범위다. 여러 worker 프로세스를 쓰면 각 프로세스의 한도를 나눠 설정하거나 queue concurrency로 host 전체 합계를 제한한다.
