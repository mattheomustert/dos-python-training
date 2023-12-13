from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String

DWHBase = declarative_base()


class dim_datum(DWHBase):
    __table_name__ = "dim_datum"

    datum_id = Column(Integer, name="datumId", primary_key=True, nullable=False)
    jaar = Column(Integer, name="jaar")


class dim_student(DWHBase):
    __table_name__ = "dim_student"

    student_id = Column(Integer, name="studentId", primary_key=True, nullable=False)
    geslacht = Column(String, name="geslacht")


class dim_diploma(DWHBase):
    __table_name__ = "dim_diploma"

    diploma_id = Column(Integer, name="diplomaId", primary_key=True, nullable=False)
    diploma_soort = Column(String, name="diplomaSoort")


class dim_opleiding(DWHBase):
    __table_name__ = "dim_opleiding"

    opleiding_id = Column(Integer, name="opleidingId", primary_key=True, nullable=False)
    opleidings_code = Column()
    opleidings_naam = Column()
    opleidings_vorm = Column()
    croho_onderdeel = Column()
    croho_subonderdeel = Column()
    is_voltijd = Column()
    is_deeltijd = Column()


class dim_onderwijsinstelling(DWHBase):
    __table_name__ = "dim_onderwijsinstelling"

    onderwijsinstellings_id = Column()
    instellingsnaam_actueel = Column()
    provincie = Column()
    gemeente_naam = Column()
    gemeente_nummer = Column()
    soort_instelling = Column()
    brin_nummer_actueel = Column()


class feit_student_studeert_af_aan_onderwijsinstelling(DWHBase):
    __table_name__ = "feit_student_studeert_af_aan_onderwijsinstelling"

    id = Column()
    datum_id = Column()
    onderwijsinstelling_id = Column()
    student_id = Column()
    opleiding_id = Column()
    diploma_id = Column()
    aantal_afgestudeerden = Column()