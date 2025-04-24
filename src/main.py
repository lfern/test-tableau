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
from scrape.exception import ScrapeError, ScrapeNoWorksheetsAfterLoad, ScrapeNoVariableProcessed
from playwright._impl._errors import Error as PlaywrightError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
pd.set_option('display.max_rows', None)

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_colwidth', None)
#pd.set_option('display.width', 0)


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
    scraper = None

    try:
        while pantalla_comunidad is not None:
            logging.info(
                f"Scrapeando pantalla: {pantalla_comunidad.pantalla.nombre}, comunidad: {pantalla_comunidad.comunidad.nombre}")

            if scraper is None:
                scraper = scrape.scrape.Scraper()
                await scraper.start()

            try:
                try:
                    await scraper.scrape(pantalla_comunidad)
                    pantalla_comunidad.set_procesado(db.session)
                    db.session.commit()
                except ScrapeNoWorksheetsAfterLoad as scrape_error:
                    logging.info("Intentando provincia a provincia")
                    for provincia in pantalla_comunidad.comunidad.provincias:

                        await scraper.scrape(pantalla_comunidad, provincia)

                    pantalla_comunidad.set_procesado(db.session)
                    db.session.commit()
            except (ScrapeNoVariableProcessed, ScrapeNoWorksheetsAfterLoad) as scrape_error:
                logging.error(f"Scrape error: {scrape_error}")
                raise
            except (ScrapeError, PlaywrightError) as scrape_error:
                logging.error(f"Scrape error: {scrape_error}")
                pantalla_comunidad.set_error(db.session, traceback.format_exc())
                db.session.commit()
                await scraper.screenshot(path="pagina_completa.png", full_page=True)
                # await db.set_pantalla_provincia_error(pantalla_provincia, scrape_error)
                await scraper.finalize()
                scraper = None
            # break
            time.sleep(5)
            pantalla_comunidad = db.get_pending_pantalla()
        #
    finally:
        if scraper is not None:
            await scraper.finalize()

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    asyncio.run(main())
    # db.mostrar_provincias()

