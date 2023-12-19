from campus_recruitment_etl.models.dwh import DimensieOpleiding
from campus_recruitment_etl.repositories.dim_opleiding import DimensieOpleidingSourceRepository, \
    DimensionOpleidingDWHRepository
from campus_recruitment_etl.transformers.base import Transformable


class DimensionOpleidingTransformer(Transformable):
    def __init__(self,
                 src_file_paths: list[str],
                 src_repository: DimensieOpleidingSourceRepository,
                 dwh_repository: DimensionOpleidingDWHRepository
                 ):
        self.files = src_file_paths
        self.src_repo = src_repository
        self.dwh_repository = dwh_repository

    def transform(self) -> None:
        df = self.src_repo.get_dataframe(self.files)
        df = self.src_repo.drop_duplicates(df)
        df = self.src_repo.add_is_voltijd(df)
        df = self.src_repo.rename_columns(df)
        df = self.src_repo.add_id_column(df)

        self.dwh_repository.insert_records_in_batches(df.to_dict(orient='records'), dwh_model=DimensieOpleiding)
