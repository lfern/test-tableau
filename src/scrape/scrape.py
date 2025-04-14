import asyncio
import enum
import json
import logging
import os
from pathlib import Path
from typing import List, Optional, TypedDict
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, FrameLocator
from tableauscraper import dashboard, TableauWorksheet
from scrape.exception import ScrapeTimeoutError, ScrapeError
from tableau.tableau_utils import TableauScraper2
from db import db


class ColumnNames(TypedDict):
    label: str
    municipio: str
    municipio2: str


class ScrapeResponse(enum.Enum):
    INITIAL = "initial"
    FIRST_RENDER = "first_render"
    SET_PARAM = "set_param"
    NEW_LAYOUT = "new_layout"
    CATEGORICAL = "categorical"

    @classmethod
    def from_string(cls, response_name: str) -> Optional["ScrapeResponse"]:
        try:
            return ScrapeResponse(response_name)
        except ValueError:
            return None


class ScrapeScreen(enum.Enum):
    DEMOGRAFIA = "Demografía"
    MEDIOFISICO = "Medio Físico"
    ECONOMIA = "Economía"
    SERVICIOS = "Servicios"
    VIVIENDA = "Vivienda"
    MEDIOAMBIENTE = "Medioambiente"

    def to_scrape_tab(self) -> "ScrapeTab":
        if ScrapeScreen.DEMOGRAFIA == self:
            return ScrapeTab.B1_DEMOGRAFICO_CCAA
        elif ScrapeScreen.MEDIOFISICO == self:
            return ScrapeTab.B2_GEOGRAFICO_CCAA
        elif ScrapeScreen.ECONOMIA == self:
            return ScrapeTab.B3_ECONOMICO_CCAA
        elif ScrapeScreen.SERVICIOS == self:
            return ScrapeTab.B4_SERVICIOS_CCAA
        elif ScrapeScreen.VIVIENDA == self:
            return ScrapeTab.B5_VIVIENDA_CCAA
        else:  # ScrapeScreen.MEDIOAMBIENTE == self:
            return ScrapeTab.B6_MEDIOAMBIENTAL_CCAA

    def get_sheet_name(self) -> str:
        if ScrapeScreen.DEMOGRAFIA == self:
            return "B1_mapa_ccaa_todas_variables"
        elif ScrapeScreen.MEDIOFISICO == self:
            return "B2_mapa_ccaa_todas_variables"
        elif ScrapeScreen.ECONOMIA == self:
            return "B3_mapa_ccaa_todas_variables"
        elif ScrapeScreen.SERVICIOS == self:
            return "B4_mapa_ccaa_todas_variables"
        elif ScrapeScreen.VIVIENDA == self:
            return "B5_mapa_ccaa_todas_variables"
        else:  # ScrapeScreen.MEDIOAMBIENTE == self:
            return "B6_mapa_ccaa_todas_variables"

    def get_column_names(self) -> ColumnNames:
        if ScrapeScreen.DEMOGRAFIA == self:
            return {
                "label": "MIN(cp_label_sheet_municipio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }
        elif ScrapeScreen.MEDIOFISICO == self:
            return {
                "label": "MIN(cp_B2_label_sheet_municpio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }
        elif ScrapeScreen.ECONOMIA == self:
            return {
                "label": "MIN(cp_B3_label_sheet_municpio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }
        elif ScrapeScreen.SERVICIOS == self:
            return {
                "label": "MIN(cp_B4_label_sheet_municpio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }
        elif ScrapeScreen.VIVIENDA == self:
            return {
                "label": "MIN(cp_B5_label_sheet_municpio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }
        else:  # ScrapeScreen.MEDIOAMBIENTE == self:
            return {
                "label": "MIN(cp_B6_label_sheet_municpio_provincia)-alias",
                "municipio": "cc_Municpio_name_after_set-alias",
                "municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
            }


    @classmethod
    def from_string(cls, screen_name: str) -> Optional["ScrapeScreen"]:
        try:
            return ScrapeScreen(screen_name)
        except ValueError:
            return None


class ScrapeTab(enum.Enum):
    PORTADA = "Portada"
    BUSQUEDA = "Búsqueda personalizada"
    B1_DEMOGRAFICO = "B1_Demográfico"
    B1_DEMOGRAFICO_2 = "B1_Demográfico_2"
    B1_DEMOGRAFICO_PROVINCIAL = "B1_Demográfico_Provincial"
    B1_DEMOGRAFICO_CCAA = "B1_Demográfico_CCAA"
    B1_DEMOGRAFICO_NACIONAL = "B1_Demográfico_Nacional"
    B2_GEOGRAFICO = "B2_Geográfico"
    B2_GEOGRAFICO_PROVINCIAL = "B2_Geográfico_Provincial"
    B2_GEOGRAFICO_CCAA = "B2_Geográfico_CCAA"
    B2_GEOGRAFICO_NACIONAL = "B2_Geográfico_Nacional"
    B3_ECONOMICO = "B3_Económico"
    B3_ECONOMICO_PROVINCIAL = "B3_Económico_Provincial"
    B3_ECONOMICO_CCAA = "B3_Económico_CCAA"
    B3_ECONOMICO_NACIONAL = "B3_Económico_Nacional"
    B4_SERVICIOS = "B4_Servicios"
    B4_SERVICIOS_2 = "B4_Servicios_2"
    B4_SERVICIOS_PROVINCIAL = "B4_Servicios_Provincial"
    B4_SERVICIOS_CCAA = "B4_Servicios_CCAA"
    B4_SERVICIOS_NACIONAL = "B4_Servicios_Nacional"
    B5_VIVIENDA = "B5_Vivienda"
    B5_VIVIENDA_PROVINCIAL = "B5_Vivienda_Provincial"
    B5_VIVIENDA_CCAA = "B5_Vivienda_CCAA"
    B5_VIVIENDA_NACIONAL = "B5_Vivienda_Nacional"
    B6_MEDIOAMBIENTAL = "B6_MedioAmbiental"
    B6_MEDIOAMBIENTAL_PROVINCIAL = "B6_Medioambiental_Provincial"
    B6_MEDIOAMBIENTAL_CCAA = "B6_Medioambiental_CCAA"
    B6_MEDIOAMBIENTAL_NACIONAL = "B6_Medioambiental_Nacional"

    def to_scrape_screen(self) -> Optional[ScrapeScreen]:
        if ScrapeTab.B1_DEMOGRAFICO_CCAA == self:
            return ScrapeScreen.DEMOGRAFIA
        elif ScrapeTab.B2_GEOGRAFICO_CCAA == self:
            return ScrapeScreen.MEDIOFISICO
        elif ScrapeTab.B3_ECONOMICO_CCAA == self:
            return ScrapeScreen.ECONOMIA
        elif ScrapeTab.B4_SERVICIOS_CCAA == self:
            return ScrapeScreen.SERVICIOS
        elif ScrapeTab.B5_VIVIENDA_CCAA == self:
            return ScrapeScreen.VIVIENDA
        elif ScrapeTab.B6_MEDIOAMBIENTAL_CCAA == self:
            return ScrapeScreen.MEDIOAMBIENTE
        return None

    @classmethod
    def from_string(cls, tab_name: str) -> Optional["ScrapeTab"]:
        try:
            return ScrapeTab(tab_name)
        except ValueError:
            return None


class Scraper:
    def __init__(self, cache_path: str = "./.cache"):
        self.logger = logging.getLogger(__name__)
        self.last_responses_found = []
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.current_ccaa: Optional[str] = None
        self.current_screen: Optional[str] = None
        self.cache_path = Path(cache_path)
        os.makedirs(self.cache_path, exist_ok=True)

    async def screenshot(self, path: str, full_page: bool = False):
        if self.page is None:
            raise ScrapeError("Page is not initialized")

        await self.page.screenshot(path=path, full_page=full_page)

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context(
            bypass_csp=True,  # Opcional: Ignorar la política de seguridad de contenido
            ignore_https_errors=True,  # Opcional: Ignorar errores de HTTPS
        )
        self.page = await self.context.new_page()
        await self.page.add_init_script(INIT_SCRIPT)
        self.current_ccaa = "16"
        self.current_screen = ScrapeScreen.DEMOGRAFIA

        self.logger.info("Go to tableau main page.")
        await self.page.goto(
            "https://public.tableau.com/app/profile/reto.demografico/viz/SistemaIntegradodeDatosMunicipales2023/B1_Demogrfico_CCAA"
        )

        await self._wait_for_response([ScrapeResponse.INITIAL, ScrapeResponse.FIRST_RENDER])
        self.logger.info("Initial responses ready!.")
        await self._close_cookies()
        self.logger.info("Cookies closed.")

    async def finalize(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def scrape(self, pantalla_comunidad: db.PantallaComunidad):
        current_screen = await self.get_current_screen()
        if current_screen is None:
            raise ScrapeError("Unknown screen found in page")

        self.logger.info(f"Current screen -> {current_screen}")
        requested_screen = ScrapeScreen.from_string(pantalla_comunidad.pantalla.nombre)
        if requested_screen is None:
            raise ScrapeError(f"Unkown requested screen {pantalla_comunidad.pantalla.nombre}")

        if current_screen != requested_screen:
            self.logger.info(f"Current screen {current_screen} isn't the expected {requested_screen}, trying to move to it")
            # we have to move to requested screen
            self._reset_last_responses()
            # we have to move to pantalla
            await self._move_to_screen(requested_screen)
            await self._wait_for_response([ScrapeResponse.NEW_LAYOUT, ScrapeResponse.FIRST_RENDER])

        current_municipio = await self.get_current_municipio()
        if current_municipio is None:
            raise ScrapeError("Municipio can't be found")

        municipio_code = current_municipio[:2]
        provincia = pantalla_comunidad.comunidad.get_provincia_capital()
        if provincia is None:
            raise ScrapeError(f"Cant find capital for comunidad {pantalla_comunidad.comunidad.nombre}")

        if municipio_code != provincia.codigo:
            self.logger.info(f"Current municipio {municipio_code} is not the expected {provincia.codigo}")
            await self._move_to_provincia(provincia.codigo)
            await self._wait_for_response([ScrapeResponse.CATEGORICAL])

        await self._proccess_all_data(requested_screen, pantalla_comunidad)

    async def _check_new_data(self, page: Page) -> List[ScrapeResponse]:
        pages_found: List[ScrapeResponse] = []

        while True:
            response = await page.evaluate(
                """() => {
                    let elem = window.top.__responses.shift();
                    if (elem) {
                        return {responseText: elem.responseText, tipo: elem.tipo};
                    }
                    return null;
                }"""
            )

            if response is None:
                break

            self.logger.info(f"Got {response['tipo']} from javascript")
            # scrape_response = ScrapeResponse.__members__.get(response['tipo'])
            scrape_response = ScrapeResponse.from_string(response['tipo'])
            if scrape_response is not None:
                self.logger.info(f"Response {scrape_response} received")
                self.last_responses_found.append({
                    'tipo': scrape_response,
                    'response': response["responseText"],
                })

                pages_found.append(scrape_response)
                if self.current_ccaa and self.current_screen:
                    filename = self.cache_path / f'{self.current_ccaa}-{self.current_screen}-{response["tipo"]}.json'
                    with open(filename, "w", encoding="utf-8") as file:
                        file.write(response["responseText"])
                        self.logger.info(f'Archivo {filename} escrito correctamente.')

        return pages_found

    async def _wait_for_response(self, responses: List[ScrapeResponse], timeout: int = 120):
        self.logger.info(f"Wait for responses {responses}")
        pending_responses = responses.copy() #
        start_time = asyncio.get_event_loop().time()  # Tiempo de inicio

        while len(pending_responses) > 0:
            # Verifica si el tiempo de espera ha superado el límite
            elapsed_time = asyncio.get_event_loop().time() - start_time
            if elapsed_time > timeout:
                raise ScrapeTimeoutError(f"Timeout waiting for responses {timeout} seconds.")

            await self.page.wait_for_timeout(3000)
            pages_found = await self._check_new_data(self.page)
            pending_responses = [item for item in pending_responses if item not in pages_found]
            self.logger.info(f"Pending responses {pending_responses}")

    async def get_current_screen(self) -> Optional[ScrapeScreen]:
        iframe = self._get_iframe_locator()
        # selector = iframe.locator('div#tabs div[wairole="presentation"] > div[wairole="presentation"] > div[wairole="presentation"] > span')
        # await selector.first.wait_for(timeout=5000)
        # textos = await selector.all_text_contents()
        # print([texto.strip() for texto in textos])

        selector = iframe.locator('div#tabs div[wairole="presentation"][aria-selected="true"] > div[wairole="presentation"] > div[wairole="presentation"] > span')
        await selector.first.wait_for(timeout=5000)
        current_screen_name = await selector.text_content()
        current_tab = ScrapeTab.from_string(current_screen_name)
        if current_tab is None:
            return None

        return current_tab.to_scrape_screen()

    async def get_current_municipio(self) -> Optional[str]:
        iframe = self._get_iframe_locator()
        selector = iframe.locator('div.CategoricalFilterBox span.tabComboBox')
        await selector.first.wait_for(timeout=5000)
        return await selector.text_content()

    def _get_iframe_locator(self) -> FrameLocator:
        return self.page.frame_locator('div#embedded-viz-wrapper iframe')

    def _reset_last_responses(self):
        self.last_responses_found = [
            item for item in self.last_responses_found if item["tipo"] == ScrapeResponse.INITIAL
        ]

    async def _move_to_screen(self, screen: ScrapeScreen):
        scrape_tab = screen.to_scrape_tab()
        iframe = self._get_iframe_locator()
        selector = iframe.locator(
            f'div#tabs div[wairole="presentation"]:not([aria-selected="true"]) >'
            f' div[wairole="presentation"] > div[wairole="presentation"] > '
            f'span:has-text("{scrape_tab.value}")'
        )
        await selector.first.wait_for(timeout=5000)

        parent_div = selector.locator('..')  # El doble punto (..) va al elemento padre en XPath
        await parent_div.click()

    async def _move_to_provincia(self, provincia_codigo: str):
        iframe = self._get_iframe_locator()
        selector = iframe.locator('div.CategoricalFilterBox span.tabComboBox')
        await selector.first.wait_for(timeout=5000)
        await selector.click()
        selector = iframe.locator('div.SearchBox textarea.QueryBox')
        await selector.first.wait_for(timeout=5000)
        await selector.first.press('Control+A')
        await selector.first.press('Backspace')
        await selector.first.fill(provincia_codigo+'001')

        selector = iframe.locator(f'div[id*="Codigo Municipio"] a[title^="{provincia_codigo}001"]')
        await selector.first.wait_for(timeout=10000)
        await selector.click()

        pass

    async def _close_cookies(self):
        # Cerrar cookies
        button = await self.page.query_selector("#onetrust-accept-btn-handler")
        if button:
            await button.click()  # Pulsar el botón
            self.logger.info("Botón de cookies pulsado correctamente.")
        else:
            self.logger.info("No se encontró el botón de cookies.")

    async def _get_current_variable(self) -> str:
        iframe = self._get_iframe_locator()
        selector = iframe.locator(
            'div.ParameterControl div[title="Inicia la navegación seleccionando una variable de este Bloque"]'
        )
        parameter_box_selector = selector.locator('xpath=ancestor::div[contains(@class, "ParameterControlBox")]')
        variable_selector = parameter_box_selector.locator(
            'div.PCContent div.tabComboBoxNameContainer div.tabComboBoxName'
        )
        await variable_selector.first.wait_for(timeout=5000)
        return await variable_selector.text_content()

    async def _proccess_all_data(self, screen: ScrapeScreen, pantalla_comunidad: db.PantallaComunidad):
        current_variable = await self._get_current_variable()
        ts = TableauScraper2(logLevel=logging.ERROR)
        if len(self.last_responses_found) == 0:
            raise ScrapeError(f'No responses got for tableau')

        self.logger.info(f"Loading {self.last_responses_found[0]['tipo']} into tableau TS")
        ts.loads2(self.last_responses_found[0]['response'])
        workbook = ts.getWorkbook()
        # print(ts.parameters)
        for response in self.last_responses_found[1:]:
            self.logger.info(f"Loading {response['tipo']} into tableau TS")
            r = json.loads(response['response'])
            workbook.updateFullData(r)
            workbook = dashboard.getWorksheetsCmdResponse(ts, r)

        for t in workbook.worksheets:
            if t.name == screen.get_sheet_name():
                self._save_ws_info(pantalla_comunidad, current_variable, t, True, screen.get_column_names())
            # else:
            #    self._save_ws_info(t, True)

    @classmethod
    def _save_ws_info(
            cls,
            pantalla_comunidad: db.PantallaComunidad,
            variable: str,
            ws: TableauWorksheet,
            print_data: bool = False,
            attrs: Optional[ColumnNames] = None
    ):
        print(f"worksheet name : {ws.name}")  # show worksheet name
        # print(ws.data) #show dataframe for this worksheet
        print(ws.getColumns())
        if print_data:
            if attrs:
                print(ws.data[[
                    attrs.get("label"),
                    attrs.get('municipio'),
                    attrs.get('municipio2')
                ]])
            else:
                print(ws.data)

        for _, row in ws.data.iterrows():
            label = row[attrs.get("label")]
            municipio = row[attrs.get('municipio')]
            db.update_or_create_pantalla_comunidad_data(
                pantalla_comunidad.pantalla,
                pantalla_comunidad.comunidad,
                municipio,
                variable,
                label
            )

        db.session.commit()


INIT_SCRIPT = """//() => {
    console.log("init");
    var open = window.XMLHttpRequest.prototype.open;
    window.top.__responses = [];

    window.XMLHttpRequest.prototype.open = function (method, url, async, user, pass) {
        console.log(url);
        const validUrls = {
            initial: 'bootstrapSession/sessions/',
            first_render: '/notify-first-client-render-occurred',
            set_param: '/set-parameter-value-from-index',
            new_layout: '/ensure-layout-for-sheet',
            categorical: '/categorical-filter-by-index'
        };

        if (url.includes("public.tableau.com")) {
            let keyFound = null;
            for (let key of Object.keys(validUrls)) {
                if (url.includes(validUrls[key])) {
                    keyFound = key;
                    break;
                }
            }

            if (keyFound) {
                console.log(url);
                this.addEventListener("readystatechange", function () {
                    console.log('ready state %s is %s', url, this.readyState);
                    if (this.readyState === 4) {
                        console.log(window.__responses);
                        if (window.top.__responses === undefined) {
                            console.log("undeffined!!!!");
                            window.top.__responses = [];
                        }
                        console.log(window.top.__responses);

                        window.top.__responses.push({
                            responseText: this.responseText,
                            readyState: this.readyState,
                            url: url,
                            method: method,
                            tipo: keyFound,
                        });
                        console.log("despues de añadir");
                        console.log(window.top.__responses);
                    }
                }, false);
            }
        }
        open.apply(this, arguments);
    };

    const originalFetch = window.fetch;
    window.fetch = async function (...args) {
        let input = args[0];
        console.log(input);
        console.log(input instanceof Request);
        console.log(input.toString());
        const url = input instanceof Request ? input.url : input.toString();

        console.log(url);
        if (url.includes('/categorical-filter-by-index')) {
            console.log('Interceptando fetch:', args[0], args[1]);
        }

        const response = await originalFetch(...args);
        if (url.includes('/categorical-filter-by-index')) {
            console.log('Respuesta de fetch:', await response.clone().json());
        }
        return response;
    };

//}"""