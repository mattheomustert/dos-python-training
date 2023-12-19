from typing import TypeVar

from campus_recruitment_etl.models.dwh import DWHBase

DATABASE_MODELS = TypeVar("DATABASE_MODELS", bound=DWHBase)
