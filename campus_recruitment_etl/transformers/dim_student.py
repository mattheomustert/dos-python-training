from campus_recruitment_etl.repositories.dim_student import DimensieStudentSourceRepository
import pandas as pd


class DimensionStudentTransformer:
    def __init__(self, src_file_paths: list[str], src_repository: DimensieStudentSourceRepository):
        self.files = src_file_paths
        self.src_repo = src_repository

    def transform(self) -> pd.DataFrame:
        df = self.src_repo.get_dataframe(self.files)
        df = self.src_repo.filter_columns_dim_student(df)
        df = self.src_repo.drop_duplicates(df)
        df = self.src_repo.add_id_column(df)
        df = self.src_repo.rename_columns(df)

        return df
