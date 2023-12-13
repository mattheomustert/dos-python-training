from dependency_injector import containers, providers
from google.cloud import storage
from dotenv import load_dotenv
from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.repositories.dim_student import DimensieStudentSourceRepository
from campus_recruitment_etl.transformers.dim_student import DimensionStudentTransformer

load_dotenv()


class ServiceContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    config.gcs.bucket_name.from_env("GCS_BUCKET")
    config.gcs.parquet_path.from_env("BUCKET_PARQUET_PATH")

    gcs_official_client = providers.Singleton(storage.Client)

    gcs_storage_client = providers.Singleton(
        GCSStorageClient,
        bucket_name=config.gcs.bucket_name(),
        official_client=gcs_official_client
    )

    dimension_student_repository = providers.Singleton(
        DimensieStudentSourceRepository,
        storage_client=gcs_storage_client,
    )

    dimension_student_transformer = providers.Factory(
        DimensionStudentTransformer,
        src_repository=dimension_student_repository
    )
