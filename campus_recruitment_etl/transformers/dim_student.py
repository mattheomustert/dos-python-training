from campus_recruitment_etl.repositories.dim_student import DimensieStudentSourceRepository, \
    DimensionStudentDWHRepository
from campus_recruitment_etl.transformers.base import Transformable


class DimensionStudentTransformer(Transformable):
    def __init__(self,
                 src_file_paths: list[str],
                 src_repository: DimensieStudentSourceRepository,
                 dwh_repository: DimensionStudentDWHRepository
                 ):
        self.files = src_file_paths
        self.src_repo = src_repository
        self.dwh_repository = dwh_repository

    def transform(self) -> None:
        df = self.src_repo.get_dataframe(self.files)
        df = self.src_repo.drop_duplicates(df)
        df = self.src_repo.rename_student_columns(df)
        df = self.src_repo.add_student_id(df)

        self.dwh_repository.insert_records(df.to_dict(orient='records'))
