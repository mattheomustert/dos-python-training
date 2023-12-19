from campus_recruitment_etl.repositories.dim_student import DimensieStudentSourceRepository, \
    DimensionStudentDWHRepository
from campus_recruitment_etl.transformers.base import Transformable


class DimensionStudentTransformer:
    def __init__(self,
                 src_file_paths: list[str],
                 src_repository: DimensieStudentSourceRepository,
                 dwh_repository: DimensionStudentDWHRepository,
                 transformer: Transformable
                 ):
        self.files = src_file_paths
        self.src_repo = src_repository
        self.dwh_repository = dwh_repository
        self.transformer = transformer

    def transform(self) -> None:
        df = self.src_repo.get_dataframe(self.files)
        df = self.src_repo.filter_columns_dim_student(df)
        df = self.src_repo.drop_duplicates(df)
        df = self.src_repo.rename_columns(df)
        df = self.src_repo.add_id_column(df)

        self.dwh_repository.insert_records(df.to_dict(orient='records'))
