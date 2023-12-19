from sqlalchemy import create_engine
from dependency_injector import resources
from sqlalchemy.engine import Engine


class ConnectionToBigQuery(resources.Resource):
    def init(self, bigquery_connection_url: str) -> Engine:
        return create_engine(bigquery_connection_url)

    def shutdown(self, engine: Engine) -> None:
        return engine.dispose()
