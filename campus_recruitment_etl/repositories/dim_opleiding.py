from sqlalchemy.engine import Engine

from campus_recruitment_etl.models.dwh import DimensieOpleiding
from campus_recruitment_etl.services.storage import GCSStorageClient
from campus_recruitment_etl.transformers.base import AbstractSourceRepository, DWHRepositoryBase
import pandas as pd


class DimensieOpleidingSourceRepository(AbstractSourceRepository):
    DIM_OPLEIDING_COLUMNS = [
        "OPLEIDINGSNAAM ACTUEEL",
        "OPLEIDINGSCODE ACTUEEL",
        "OPLEIDINGSVORM",
        "CROHO ONDERDEEL",
        "CROHO SUBONDERDEEL"
    ]

    UNIQUE_OPLEIDING_IDENTIFIER = ["OPLEIDINGSCODE ACTUEEL", "OPLEIDINGSVORM"]

    OPLEIDING_ID_COLUMN = "opleidingId"

    OPLEIDING_COLUMNS_TO_RENAME = {
        "OPLEIDINGSNAAM ACTUEEL": DimensieOpleiding.opleidings_naam.expression.key,
        "OPLEIDINGSCODE ACTUEEL": DimensieOpleiding.opleidings_code.expression.key,
        "OPLEIDINGSVORM": DimensieOpleiding.opleidings_vorm.expression.key,
        "CROHO ONDERDEEL": DimensieOpleiding.croho_onderdeel.expression.key,
        "CROHO SUBONDERDEEL": DimensieOpleiding.croho_subonderdeel.expression.key
    }

    def __init__(self, gcs_client: GCSStorageClient):
        super().__init__(gcs_client)

    def get_dataframe(self, parquet_files: list[str]) -> pd.DataFrame:
        return self._get_dataframe(parquet_files=parquet_files, dim_columns=self.DIM_OPLEIDING_COLUMNS)

    def drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._drop_duplicates(df, subset=self.UNIQUE_OPLEIDING_IDENTIFIER)

    def add_id_column(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._add_id_column(df, self.OPLEIDING_ID_COLUMN)

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._rename_columns(df, columns_to_rename=self.OPLEIDING_COLUMNS_TO_RENAME)

    @staticmethod
    def add_is_voltijd(df: pd.DataFrame) -> pd.DataFrame:
        df["isVoltijd"] = df["OPLEIDINGSVORM"].isin(["voltijd"])
        return df


class DimensionOpleidingDWHRepository(DWHRepositoryBase):
    def __init__(self, bigquery_connection_engine: Engine):
        super().__init__(bigquery_connection_engine)
