import sys
from typing import List, Type, Optional

from sqlalchemy import create_engine, Enum, DateTime, Column, Integer, String, ForeignKey, Boolean, asc, \
    UniqueConstraint, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Mapped, Session
from datetime import datetime
import enum


class Estado(enum.Enum):
    PENDIENTE = "pendiente"
    PROCESADO = "procesado"
    ERROR = "error"


Base = declarative_base()


class Comunidad(Base):
    __tablename__ = 'comunidades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(2), unique=True, nullable=False)
    nombre = Column(String)

    provincias = relationship('Provincia', back_populates='comunidad')
    pantalla_comunidades = relationship('PantallaComunidad', back_populates='comunidad')
    pantalla_comunidad_datas = relationship('PantallaComunidadData', back_populates='comunidad')

    def get_provincia_capital(self) -> "Provincia":
        return next((p for p in self.provincias if p.es_capital), None)

    def get_codigo_provincia_capital(self):
        capital = self.get_provincia_capital()
        return capital.codigo if capital else None


class Provincia(Base):
    __tablename__ = 'provincias'

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(2), unique=True, nullable=False)
    nombre = Column(String)
    comunidad_id = Column(Integer, ForeignKey('comunidades.id'), nullable=False)
    es_capital = Column(Boolean, nullable=False, default=False)

    comunidad = relationship('Comunidad', back_populates='provincias')

    def get_provincia_capital(self):
        return next((p for p in self.provincias if p.es_capital), None)

    def get_codigo_provincia_capital(self):
        capital = self.get_provincia_capital()
        return capital.codigo if capital else None


class Pantalla(Base):
    __tablename__ = 'pantallas'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String, unique=True, nullable=False)
    descripcion = Column(String)

    pantalla_comunidades = relationship('PantallaComunidad', back_populates='pantalla')
    pantalla_comunidad_datas = relationship('PantallaComunidadData', back_populates='pantalla')


class PantallaComunidad(Base):
    __tablename__ = 'pantalla_comunidad'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_pantalla = Column(Integer, ForeignKey('pantallas.id'))
    id_comunidad = Column(Integer, ForeignKey('comunidades.id'))
    estado = Column(Enum(Estado, native_enum=False), nullable=False, default=Estado.PENDIENTE)
    fecha_estado = Column(DateTime, nullable=False, default=datetime.utcnow)
    error = Column(Text, nullable=True)
    error_count = Column(Integer, nullable=False, default=0)

    pantalla: Mapped[Pantalla] = relationship('Pantalla', back_populates='pantalla_comunidades')
    comunidad: Mapped[Comunidad] = relationship('Comunidad', back_populates='pantalla_comunidades')

    def set_procesado(self, sess: Session):
        self.estado = Estado.PROCESADO
        self.fecha_estado = datetime.utcnow()
        self.error = None
        sess.add(self)

    def set_error(self, sess: Session, mensaje: str):
        self.estado = Estado.ERROR
        self.fecha_estado = datetime.utcnow()
        self.error = mensaje
        self.error_count += 1
        sess.add(self)


class PantallaComunidadData(Base):
    __tablename__ = 'pantalla-comunidad-data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_pantalla = Column(Integer, ForeignKey('pantallas.id'))
    id_comunidad = Column(Integer, ForeignKey('comunidades.id'))
    municipio = Column(String, nullable=False)
    nombre = Column(String, nullable=False)
    valor = Column(String)
    fecha_descarga = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relación con las otras tablas
    pantalla = relationship('Pantalla', back_populates='pantalla_comunidad_datas')
    comunidad = relationship('Comunidad', back_populates='pantalla_comunidad_datas')

    # Agregar la restricción UNIQUE
    __table_args__ = (
        UniqueConstraint('id_pantalla', 'id_comunidad', 'municipio', 'nombre', name='uix_pantalla_comunidad_nombre'),
    )


# Crear motor y sesión
engine = create_engine('sqlite:///database.db', echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# Crear las tablas en la base de datos (si no existen)
Base.metadata.create_all(engine)


def update_or_create_comunidad(codigo: str, nombre: str) -> Comunidad:
    comunidad = session.query(Comunidad).filter_by(codigo=codigo).first()
    if comunidad:
        comunidad = Comunidad(
            id=comunidad.id,
            codigo=codigo,
            nombre=nombre,
        )

        session.merge(comunidad)
    else:
        comunidad = Comunidad(
            codigo=codigo,
            nombre=nombre,
        )
        session.add(comunidad)

    return comunidad


def update_or_create_provincia(codigo: str, nombre: str, comunidad: Comunidad, es_capital:bool) -> Provincia:
    provincia = session.query(Provincia).filter_by(codigo=codigo).first()
    if provincia:
        provincia = Provincia(
            id=provincia.id,
            codigo=codigo,
            nombre=nombre,
            comunidad=comunidad,
            es_capital=es_capital
        )

        session.merge(provincia)
    else:
        provincia = Provincia(
            codigo=codigo,
            nombre=nombre,
            comunidad=comunidad,
            es_capital=es_capital
        )

        session.add(provincia)

    return provincia


def update_or_create_pantalla(nombre: str, descripcion: str) -> Pantalla:
    pantalla = session.query(Pantalla).filter_by(nombre=nombre).first()
    if pantalla:
        pantalla = Pantalla(
            id=pantalla.id,
            nombre=nombre,
            descripcion=descripcion,
        )

        session.merge(pantalla)
    else:
        pantalla = Pantalla(
            nombre=nombre,
            descripcion=descripcion,
        )

        session.add(pantalla)

    return pantalla


# Obtener todas las provincias
def get_provincias() -> List[Type[Provincia]]:
    return session.query(Provincia).all()


def get_comunidades() -> List[Type[Comunidad]]:
    return session.query(Comunidad).all()


def update_or_create_pantalla_comunidad(pantalla: Pantalla, comunidad: Type[Comunidad]) -> PantallaComunidad:
    pantalla_comunidad = session.query(PantallaComunidad).filter_by(pantalla=pantalla, comunidad=comunidad).first()
    if pantalla_comunidad is None:
        pantalla_comunidad = PantallaComunidad(pantalla=pantalla, comunidad=comunidad, estado=Estado.PENDIENTE)
        session.add(pantalla_comunidad)

    return pantalla_comunidad


def update_or_create_pantalla_comunidad_data(pantalla: Pantalla, comunidad: Comunidad, municipio: str, variable: str, value: str) -> (
        PantallaComunidadData):
    pantalla_comunidad_data = session.query(PantallaComunidadData).filter_by(
        pantalla=pantalla,
        comunidad=comunidad,
        municipio=municipio,
        nombre=variable,
    ).first()
    if pantalla_comunidad_data:
        pantalla_comunidad_data.municipio = municipio
        pantalla_comunidad_data.nombre = variable
        pantalla_comunidad_data.valor = value
        pantalla_comunidad_data.fecha_descarga = datetime.utcnow()
    else:
        pantalla_comunidad_data = PantallaComunidadData(
            pantalla=pantalla,
            comunidad=comunidad,
            municipio=municipio,
            nombre=variable,
            valor=value,
            fecha_descarga=datetime.utcnow(),
        )

        session.add(pantalla_comunidad_data)
    sys.stdout.flush()
    return pantalla_comunidad_data


# Función de ejemplo para obtener y mostrar todas las provincias
def mostrar_provincias():
    provincias = get_provincias()
    for provincia in provincias:
        print(f"Provincia: {provincia.nombre}, Capital: {provincia.es_capital}")


def get_pending_pantalla() -> Optional[PantallaComunidad]:
    pantalla_comunidad = (
        session.query(PantallaComunidad)
        .filter(PantallaComunidad.estado != Estado.PROCESADO)
        .filter(PantallaComunidad.error_count < 3)
        .order_by(asc(PantallaComunidad.fecha_estado))
        .first()
    )

    return pantalla_comunidad
