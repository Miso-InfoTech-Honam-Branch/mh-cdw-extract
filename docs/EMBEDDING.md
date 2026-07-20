# 작업 실행 서비스에 내장하는 방법

## 전제

`mh-cdw-extract`의 핵심 코드는 독립 웹 서비스가 아니라 다른 작업 실행 서비스에 탑재할 수
있는 Python 모듈이다. 루트의 `app.py`는 개발·통합 테스트용 HTTP 어댑터일 뿐이며, 운영
호스트가 FastAPI나 `BackgroundTasks`를 사용할 필요는 없다.

```text
Boot
  └─ HTTP 작업 요청
       └─ 운영 호스트의 API/작업 라우터
            ├─ prepare_*_job(...)  # 검증, jobId 발급, 접수 상태 저장
            └─ 백그라운드 큐
                 └─ run_*_job(...) # 실제 모듈 실행
                      └─ Boot callback API 호출
```

## 모듈 호출 규칙

1. 호스트 API는 요청을 받은 스레드에서 `prepare_*_job`만 호출한다.
2. prepare 결과의 `jobId`와 접수 응답을 Boot에 즉시 반환한다.
3. 실제 `run_*_job` 호출은 호스트의 큐나 백그라운드 실행기가 담당한다.
4. 완료·실패·취소 결과는 요청에 포함된 callback 계약으로 Boot에 알린다.
5. 콜백 실패는 작업 결과를 실패로 되돌리지 않는다. 재전송 또는 Boot의 상태 대사가 가능하도록
   job manifest에 별도로 기록한다.

현재 공개 진입점은 `cdw_extract/__init__.py`에서 확인한다. 동기 분석 조회처럼 짧은 작업은
`run_analytics_query`를 직접 호출할 수 있지만, 추출·동기화·파일 변환·결과 파일 생성은
prepare/run 경계를 유지한다.

## 호스트가 담당할 것

- API 인증과 Boot 호출 권한 검증
- 작업 유형에서 실행 함수로의 라우팅
- 스레드·프로세스·외부 큐 선택과 동시 실행 제한
- 프로세스 재시작 후 미완료 작업 복구
- 로그 수집, 추적 ID, 운영 메트릭
- 데이터 루트와 비밀값 주입

모듈은 호스트 프레임워크 객체, 전역 DI 컨테이너 또는 특정 큐 메시지 클래스에 의존하면 안 된다.
요청 모델·일반 Python 값·파일 경로를 입력받고 직렬화 가능한 결과 또는 명확한 예외를 반환한다.

## Boot callback 계약

Boot는 작업 유형별 callback API에서 다음을 수행한다.

- `jobId`, 대상 리소스 ID와 현재 실행 ID가 일치하는지 확인
- 동일한 완료 callback이 다시 와도 같은 결과가 되는 멱등 처리
- 이미 완료된 작업을 늦게 도착한 RUNNING/FAILED 상태로 되돌리지 않기
- 결과 파일, 행 수, 컬럼 등 작업별 결과 메타데이터 저장
- callback 인증정보와 원천 DB 비밀값을 로그에 남기지 않기

callback payload를 변경할 때에는 Boot DTO·컨트롤러 테스트와 해당 모듈 테스트를 같은 변경으로
관리한다.

## 앞으로 통일할 인터페이스

현재 추출, 메타데이터 동기화, 사용자 파일 변환, 분석 결과 파일은 같은 prepare/run 개념을
사용하지만 함수 인자와 callback 전송 코드가 일부 다르다. 호스트 프로젝트를 확정할 때 다음의
작은 어댑터 규약으로 통일하는 것이 좋다.

```python
class JobModule(Protocol):
    def prepare(self, request, context) -> AcceptedJob: ...
    def run(self, accepted_job, request, context) -> None: ...
    def cancel(self, job_id, context) -> JobStatus: ...
```

이 인터페이스는 호스트의 큐 구현을 모듈 안으로 가져오기 위한 것이 아니라, 호스트 라우터가
작업 유형마다 동일한 방식으로 모듈을 찾고 호출하기 위한 경계다.

