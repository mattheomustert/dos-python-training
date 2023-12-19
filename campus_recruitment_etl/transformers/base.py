import io
from abc import abstractmethod, ABCMeta

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from campus_recruitment_etl.models.dwh import DimensieStudent
from campus_recruitment_etl.services.storage import GCSStorageClient


class Transformable:
    @abstractmethod
    def transform(self) -> None:
        pass


class DWHRepositoryBase:
    def __init__(self, bigquery_connection_engine: Engine):
        self.bigquery_connection_engine = bigquery_connection_engine

    def insert_records(self, transformed_df: list[dict]):
        # Inject whole datamodel
        truncate_query = f"TRUNCATE TABLE {DimensieStudent.__tablename__}"
        self.bigquery_connection_engine.execute(truncate_query)

        insert_query = insert(DimensieStudent).values(transformed_df)
        self.bigquery_connection_engine.execute(insert_query)


class AbstractSourceRepository:
    # TODO: make a private methods so that method names can be re-usable
    def __init__(self, gcs_client: GCSStorageClient):
        self.gcs_client = gcs_client

    def _get_dataframe(self, parquet_files: list[str], dim_columns: list[str]) -> pd.DataFrame:
        dfs = []

        for file in parquet_files:
            file_byte_contents = self.gcs_client.download_file(file)
            df = pd.read_parquet(io.BytesIO(file_byte_contents), columns=dim_columns)
            dfs.append(df)

        return pd.concat(dfs)

    @staticmethod
    def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(keep="first")

    @staticmethod
    def add_id_column(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        df[id_column] = df.index + 1
        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame, columns_to_rename: dict) -> pd.DataFrame:
        return df.rename(columns=columns_to_rename)
