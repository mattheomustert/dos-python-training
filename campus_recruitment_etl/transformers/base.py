from abc import abstractmethod
from sqlalchemy.engine import Engine


class Transformable:
    @abstractmethod
    def transform(self) -> None:
        pass


class DWHRepositoryBase:
    def __init__(self, bigquery_connection_engine: Engine):
        self.bigquery_connection_engine = bigquery_connection_engine

