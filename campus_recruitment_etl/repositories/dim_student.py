import pandas as pd
from sqlalchemy.engine import Engine
from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.transformers.base import DWHRepositoryBase, AbstractSourceRepository


class DimensieStudentSourceRepository(AbstractSourceRepository):
    DIM_STUDENT_COLUMNS = ["GESLACHT"]
    STUDENT_ID_COLUMN = "studentId"
    STUDENT_COLUMNS_TO_RENAME = {"GESLACHT": "geslacht"}

    def __init__(self, gcs_client: GCSStorageClient):
        super().__init__(gcs_client)

    def get_dataframe(self, parquet_files: list[str]) -> pd.DataFrame:
        return self._get_dataframe(parquet_files=parquet_files, dim_columns=self.DIM_STUDENT_COLUMNS)

    def drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._drop_duplicates(df)

    def add_id_column(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._add_id_column(df, id_column=self.STUDENT_ID_COLUMN)

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._rename_columns(df, columns_to_rename=self.STUDENT_COLUMNS_TO_RENAME)


class DimensionStudentDWHRepository(DWHRepositoryBase):
    def __init__(self, bigquery_connection_engine: Engine):
        super().__init__(bigquery_connection_engine)
