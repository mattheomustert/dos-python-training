from dependency_injector import containers, providers
from google.cloud import storage
from dotenv import load_dotenv
from campus_recruitment_etl.services.storage import GCSStorageClient

load_dotenv()


class ServiceContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    config.gcs.bucket_name.from_env("GCS_BUCKET")

    gcs_official_client = providers.Singleton(storage.Client)

    gcs_storage_client = providers.Singleton(
        GCSStorageClient,
        bucket_name=config.gcs.bucket_name(),
        official_client=gcs_official_client
    )

