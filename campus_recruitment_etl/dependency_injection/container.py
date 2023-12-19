from dependency_injector import containers, providers
from google.cloud import storage
from dotenv import load_dotenv
from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.repositories.dim_student import DimensieStudentSourceRepository, \
    DimensionStudentDWHRepository
from campus_recruitment_etl.transformers.dim_student import DimensionStudentTransformer
from campus_recruitment_etl.dependency_injection.resources import ConnectionToBigQuery

load_dotenv()


class ServiceContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    config.gcs.bucket_name.from_env("GCS_BUCKET")
    config.gcs.parquet_path.from_env("BUCKET_PARQUET_PATH")
    config.bigquery.connection_url.from_env("BIGQUERY_CONNECTION_URL")

    gcs_official_client = providers.Singleton(storage.Client)

    gcs_storage_client = providers.Singleton(
        GCSStorageClient,
        bucket_name=config.gcs.bucket_name(),
        official_client=gcs_official_client
    )

    dimension_student_repository = providers.Singleton(
        DimensieStudentSourceRepository,
        gcs_client=gcs_storage_client
    )

    connection_to_bigquery = providers.Resource(
        ConnectionToBigQuery,
        bigquery_connection_url=config.bigquery.connection_url()
    )

    dimension_student_dwh_repository = providers.Singleton(
        DimensionStudentDWHRepository,
        bigquery_connection_engine=connection_to_bigquery
    )

    dimension_student_transformer = providers.Factory(
        DimensionStudentTransformer,
        src_repository=dimension_student_repository,
        dwh_repository=dimension_student_dwh_repository
    )

