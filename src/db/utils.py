import json
from pathlib import Path
import sys
from sqlalchemy.exc import SQLAlchemyError

from db import db


def insert_all_provincias():
    try:
        file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "ccaa.json"
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

            for comunidadJson in data:
                comunidad = db.update_or_create_comunidad(
                    codigo=comunidadJson['codigo'],
                    nombre=comunidadJson['nombre'],
                )

                for provinciaJson in comunidadJson['provincias']:
                    provincia = db.update_or_create_provincia(
                        codigo=provinciaJson['codigo'],
                        nombre=provinciaJson['nombre'],
                        comunidad=comunidad,
                        es_capital=provinciaJson.get('es_capital', False)
                    )

            db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        print(f"Error al insertar o actualizar las comunidades/provincias: {e}")
        raise


def insert_all_pantallas():
    try:
        file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "pantallas.json"
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        for pantallaJson in data:
            pantalla = db.update_or_create_pantalla(
                nombre=pantallaJson['nombre'],
                descripcion=pantallaJson['descripcion'],
            )

            for comunidad in db.get_comunidades():
                db.update_or_create_pantalla_comunidad(pantalla, comunidad)

        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        print(f"Error al insertar o actualizar las comunidades/provincias: {e}")
        raise