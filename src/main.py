import asyncio
import io
import logging
import sys
import time
import traceback

import pandas as pd
import scrape.scrape
from db.utils import insert_all_pantallas, insert_all_provincias
from db import db
from scrape.exception import ScrapeError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# pd.set_option('display.max_rows', None)


def init_tables():
    insert_all_provincias()
    insert_all_pantallas()


async def main():
    """
        Split the job in small pieces. A job is a CCAA and one of these:
        Demografía, Medio Físico, Economío, Servicios, Vivienda, Medioambiente.
        So in the
    :return:
    """
    init_tables()

    # obtener una pantalla pendiente
    pantalla_comunidad = db.get_pending_pantalla()
    scraper = scrape.scrape.Scraper()
    await scraper.start()

    try:
        while pantalla_comunidad is not None:
            logging.info(
                f"Scrapeando pantalla: {pantalla_comunidad.pantalla.nombre}, comunidad: {pantalla_comunidad.comunidad.nombre}")

            try:
                await scraper.scrape(pantalla_comunidad)

                #db.set_pantalla_provincia_scraped(pantalla_provincia)
                pantalla_comunidad.set_procesado(db.session)
                db.session.commit()
            except ScrapeError as scrape_error:
                print(f"Scrape error: {scrape_error}")
                pantalla_comunidad.set_error(db.session, traceback.format_exc())
                db.session.commit()
                await scraper.screenshot(path="pagina_completa.png", full_page=True)
                #await db.set_pantalla_provincia_error(pantalla_provincia, scrape_error)
                raise

            time.sleep(5)
            pantalla_provincia = db.get_pending_pantalla()
        #
    finally:
        await scraper.finalize()

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    asyncio.run(main())
    # db.mostrar_provincias()

