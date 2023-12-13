from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey

DWHBase = declarative_base()


class DimemsieDatum(DWHBase):
    __table_name__ = "dim_datum"

    datum_id = Column(Integer, name="datumId", primary_key=True, nullable=False)
    jaar = Column(Integer, name="jaar")

    datums = relationship("FeitStudentStudeertAfAanOnderwijsinstelling", back_populates="datum")


class DimensieStudent(DWHBase):
    __table_name__ = "dim_student"

    student_id = Column(Integer, name="studentId", primary_key=True, nullable=False)
    geslacht = Column(String, name="geslacht")

    students = relationship("FeitStudentStudeertAfAanOnderwijsinstelling", back_populates="student")


class DimensieDiploma(DWHBase):
    __table_name__ = "dim_diploma"

    diploma_id = Column(Integer, name="diplomaId", primary_key=True, nullable=False)
    diploma_soort = Column(String, name="diplomaSoort")

    diplomas = relationship("FeitStudentStudeertAfAanOnderwijsinstelling", back_populates="diploma")


class DimensieOpleiding(DWHBase):
    __table_name__ = "dim_opleiding"

    opleiding_id = Column(Integer, name="opleidingId", primary_key=True, nullable=False)
    opleidings_code = Column(Integer, name="opleidingsCode")
    opleidings_naam = Column(String, name="opleidingsNaam")
    opleidings_vorm = Column(String, name="opleidingsVorm")
    croho_onderdeel = Column(String, name="crohoOnderdeel")
    croho_subonderdeel = Column(String, name="crohoSubOnderdeel")
    is_voltijd = Column(Boolean, name="isVoltijd")

    opleidingen = relationship("FeitStudentStudeertAfAanOnderwijsinstelling", back_populates="diploma")


class DimensieOnderwijsinstelling(DWHBase):
    __table_name__ = "dim_onderwijsinstelling"

    onderwijsinstellings_id = Column(Integer, name="onderwijsInstellingsId", primary_key=True, nullable=False)
    instellingsnaam_actueel = Column(String, name="instellingsnaamActueel")
    provincie = Column(String, name="provincie")
    gemeente_naam = Column(String, name="gemeenteNaam")
    gemeente_nummer = Column(Integer, name="gemeenteNummer")
    soort_instelling = Column(String, name="soortInstelling")
    brin_nummer_actueel = Column(String, name="brinNummerActueel")

    onderwijsinstellingen = relationship("FeitStudentStudeertAfAanOnderwijsinstelling",
                                         back_populates="onderwijsinstelling")


class FeitStudentStudeertAfAanOnderwijsInstelling(DWHBase):
    __table_name__ = "feit_student_studeert_af_aan_onderwijsinstelling"

    id = Column(Integer, name="id", primary_key=True, nullable=False)

    datum_id = Column(Integer, ForeignKey("DimensieDatum.datumId"), name="datumId", nullable=False)
    datum = relationship("dim_datum", back_populates="datums")

    onderwijsinstelling_id = Column(Integer, ForeignKey("DimensieOnderwijsinstelling.onderwijsinstelling_id"),
                                    name="onderwijsinstellingId", nullable=False)
    onderwijsinstelling = relationship("DimensieOnderwijsinstelling", back_populates="onderwijsinstellingen")

    student_id = Column(Integer, ForeignKey("DimensieStudent.studenId"), name="studentId", nullable=False)
    student = relationship("DimensieStudent", back_populates="studenten")

    opleiding_id = Column(Integer, ForeignKey("DimensieOpleiding.opleidingId"),
                          name="opleidingId", nullable=False)
    opleiding = relationship("DimensieOpleiding", back_populates="opleidingen")

    diploma_id = Column(Integer, ForeignKey("DimensieDiploma.diplomaId"), name="dimplomaId", nullable=False)
    diploma = relationship("DimensieDiploma", back_populates="diplomas")

    aantal_afgestudeerden = Column(Integer, name="aantalAfgestudeerden")
