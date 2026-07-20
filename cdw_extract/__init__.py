"""운영 작업 호스트에서 호출할 수 있는 추출 모듈의 공개 진입점.

긴 작업은 ``prepare_*_job``으로 접수한 뒤 호스트의 백그라운드 실행기에서 대응하는
``run_*_job``을 호출한다. 이 패키지는 FastAPI나 특정 작업 큐에 의존하지 않는다.
"""

from .extract import extract, prepare_extract_job, run_extract_job
from .jobs import cancel_job, job_download_file, load_job
from .manifest import delete_connection, load_connection_manifest
from .preview import preview
from .refresh import prepare_refresh_tables_job, refresh_tables, run_refresh_tables_job
from .user_dataset_jobs import prepare_user_dataset_convert_job, run_user_dataset_convert_job
from .user_dataset import (
    convert_user_dataset_file,
    convert_user_dataset_file_from_path,
    delete_user_dataset_file,
    load_dataset_file_manifest,
    post_callback,
)

__all__ = [
    "cancel_job",
    "convert_user_dataset_file",
    "convert_user_dataset_file_from_path",
    "delete_connection",
    "delete_user_dataset_file",
    "extract",
    "job_download_file",
    "load_job",
    "load_connection_manifest",
    "load_dataset_file_manifest",
    "post_callback",
    "prepare_extract_job",
    "prepare_refresh_tables_job",
    "prepare_user_dataset_convert_job",
    "preview",
    "refresh_tables",
    "run_refresh_tables_job",
    "run_extract_job",
    "run_user_dataset_convert_job",
]
