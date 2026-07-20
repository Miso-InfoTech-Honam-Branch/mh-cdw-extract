# 추출·분석 서버 개발 가이드

## 이 프로젝트의 책임

`mh-cdw-extract`는 Boot가 전달한 제한된 요청 모델을 실행한다. 원천 데이터 동기화, Parquet
생성·조회, 분석 DSL 컴파일, 결과 파일 생성이 이 프로젝트의 경계다. 업무 데이터나 사용자별
분석 정의의 원본은 이 모듈이 소유하지 않는다. 운영 환경에서는 별도 작업 실행 서비스에
탑재하며, 루트의 `app.py`는 개발·통합 테스트용 얇은 HTTP 어댑터다.

운영 호스트와의 prepare/run/callback 경계는 [모듈 내장 가이드](EMBEDDING.md)를 따른다.

## 변경 위치 찾기

| 변경 목적 | 먼저 볼 모듈 |
| --- | --- |
| API 요청·응답 스키마 | `analytics_models.py` |
| 분석 DSL을 SQL로 변환 | `analytics_compiler.py` |
| 분석 실행 | `analytics.py` |
| PNG/PDF/XLSX 결과 파일 | `analytics_artifacts.py` |
| callback HTTP 전송·재시도 | `callback.py` |
| 사용자 데이터셋 Parquet | `user_dataset.py` |
| 메타데이터 동기화 | `refresh.py` |
| 비동기 작업 상태 | `jobs.py` |

`analytics_compiler.py`는 SQL 문자열을 받는 모듈이 아니다. 반드시 검증된 모델과 식별자 인용,
바인딩 파라미터를 사용해 폐쇄형 DSL이라는 보안 경계를 유지한다.

## 데이터 표준과 작업 계약

- Boot API의 설명형 `camelCase` 필드는 Pydantic alias로 받고, Python 내부에서는 같은 개념의
  `snake_case`를 사용한다.
- `analysisArtifactId`처럼 대상이 분명한 이름을 사용하고 `artifactId` 같은 축약 계약을 새로 만들지 않는다.
- 요청, 접수 응답, job manifest, callback에서 동일한 개념은 동일한 JSON 필드명을 사용한다.
- 현재는 DB와 작업 파일을 초기화하는 개발 단계이므로 구형 필드 fallback이나 이중 직렬화를 두지 않는다.
- 계약 변경은 Boot DTO, Vue 실제·Mock API, Extract 모델과 테스트를 같은 변경으로 처리한다.
- 영속 DB의 표준 용어·약어는 Boot의 `docs/datadictionary`를 기준으로 확인한다.

## 코드 작성 원칙

- HTTP 라우터와 작업 큐에는 조합만 두고 파일·쿼리 로직은 `cdw_extract` 모듈에 둔다.
- 핵심 모듈이 FastAPI, BackgroundTasks 또는 운영 호스트의 큐 클래스에 의존하지 않게 한다.
- 사용자 값은 SQL 문자열에 직접 이어 붙이지 않고 파라미터로 전달한다.
- 파일 경로는 기존 `safe_segment` 계열 검증을 통과시킨다.
- 긴 작업은 취소 여부를 주기적으로 검사하고 임시 파일을 원자적으로 교체한다.
- 작업 모듈은 callback payload와 오류 기록만 담당하고 HTTP 전송은 `callback.py`를 사용한다.
- Pydantic 모델 변경 시 Boot DTO와 Vue 직렬화 코드를 함께 수정한다.
- 주석과 docstring에는 동시성, 보안 경계, 포맷 제약처럼 코드만으로 드러나지 않는 이유를 쓴다.

## 검증

```powershell
python -m pytest -q
```

Python 3.11 이상이 필요하다. 새 분석 연산자는 정상 입력뿐 아니라 잘못된 타입, 존재하지 않는
컬럼, SQL 주입 형태의 입력, 결과 제한 케이스를 함께 테스트한다.
