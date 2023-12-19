import io
from abc import abstractmethod
from typing import Type

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.types.models import DATABASE_MODELS


class Transformable:
    @abstractmethod
    def transform(self) -> None:
        pass


class DWHRepositoryBase:
    def __init__(self, bigquery_connection_engine: Engine):
        self.bigquery_connection_engine = bigquery_connection_engine

    def insert_records(self, transformed_df: list[dict], dwh_model: Type[DATABASE_MODELS]):
        truncate_query = f"TRUNCATE TABLE {dwh_model.__tablename__}"
        self.bigquery_connection_engine.execute(truncate_query)

        insert_query = insert(dwh_model).values(transformed_df)
        self.bigquery_connection_engine.execute(insert_query)

    def insert_records_in_batches(self, transformed_df: list[dict], dwh_model: Type[DATABASE_MODELS], batch_size=1000):
        truncate_query = f"TRUNCATE TABLE {dwh_model.__tablename__}"
        self.bigquery_connection_engine.execute(truncate_query)

        insert_query = insert(dwh_model)
        self.bigquery_connection_engine.execute(insert_query, transformed_df,
                                                executions_options={"insertmanyvalues_page_size": batch_size})


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
    def _drop_duplicates(df: pd.DataFrame, subset=None) -> pd.DataFrame:
        return df.drop_duplicates(subset, keep="first")

    @staticmethod
    def _add_id_column(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        df[id_column] = df.index + 1
        return df

    @staticmethod
    def _rename_columns(df: pd.DataFrame, columns_to_rename: dict) -> pd.DataFrame:
        return df.rename(columns=columns_to_rename)
