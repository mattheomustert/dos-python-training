import pandas as pd
import io

from sqlalchemy import insert
from sqlalchemy.engine import Engine
from campus_recruitment_etl.models.dwh import DimensieStudent
from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.transformers.base import DWHRepositoryBase


class DimensieStudentSourceRepository:
    DIM_STUDENT_COLUMNS = ["GESLACHT"]

    def __init__(self, storage_client: GCSStorageClient, dim_student_dwh_repository: DWHRepositoryBase):
        self.storage_client = storage_client
        self.dim_student_dwh_repository = dim_student_dwh_repository

    def get_dataframe(self, parquet_files: list[str]) -> pd.DataFrame:
        dfs = []

        for file in parquet_files:
            file_byte_contents = self.storage_client.download_file(file)
            df = pd.read_parquet(io.BytesIO(file_byte_contents))
            dfs.append(df)

        return pd.concat(dfs)

    @classmethod
    def filter_columns_dim_student(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df[cls.DIM_STUDENT_COLUMNS]

    @classmethod
    def drop_duplicates(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=cls.DIM_STUDENT_COLUMNS, keep="first")

    @staticmethod
    def add_id_column(df: pd.DataFrame) -> pd.DataFrame:
        df["studentId"] = df.index + 1
        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"GESLACHT": "geslacht"})


class DimensionStudentDWHRepository:
    def __init__(self, bigquery_connection_engine: Engine):
        self.bigquery_connection_engine = bigquery_connection_engine

    def insert_records(self, transformed_df: list[dict]):
        # Inject whole datamodel
        truncate_query = f"TRUNCATE TABLE {DimensieStudent.__tablename__}"
        self.bigquery_connection_engine.execute(truncate_query)

        insert_query = insert(DimensieStudent).values(transformed_df)
        self.bigquery_connection_engine.execute(insert_query)
