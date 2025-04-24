import asyncio
import enum
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, TypedDict
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, FrameLocator, Locator
from tableauscraper import dashboard, TableauWorksheet
from scrape.exception import ScrapeTimeoutError, ScrapeError, ScrapeNoWorksheetsAfterLoad, ScrapeNoVariableProcessed
from tableau.tableau_utils import TableauScraper2
from db import db
from utils.text_utils import fix_mojibake


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

    def to_scrape_tab(self, provincia: bool) -> "ScrapeTab":
        if provincia:
            if ScrapeScreen.DEMOGRAFIA == self:
                return ScrapeTab.B1_DEMOGRAFICO_PROVINCIAL
            elif ScrapeScreen.MEDIOFISICO == self:
                return ScrapeTab.B2_GEOGRAFICO_PROVINCIAL
            elif ScrapeScreen.ECONOMIA == self:
                return ScrapeTab.B3_ECONOMICO_PROVINCIAL
            elif ScrapeScreen.SERVICIOS == self:
                return ScrapeTab.B4_SERVICIOS_PROVINCIAL
            elif ScrapeScreen.VIVIENDA == self:
                return ScrapeTab.B5_VIVIENDA_PROVINCIAL
            else:  # ScrapeScreen.MEDIOAMBIENTE == self:
                return ScrapeTab.B6_MEDIOAMBIENTAL_PROVINCIAL
        else:
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

    def get_sheet_name(self, provincia: bool) -> str:
        if provincia:
            if ScrapeScreen.DEMOGRAFIA == self:
                return "B1_deciles_op2.1"
            elif ScrapeScreen.MEDIOFISICO == self:
                return "B2_mapa_todas_variables"
            elif ScrapeScreen.ECONOMIA == self:
                return "B3_mapa_todas_var"
            elif ScrapeScreen.SERVICIOS == self:
                return "B4_deciles_map_prov_op3"
            elif ScrapeScreen.VIVIENDA == self:
                return "B5_mapa_todas_variables"
            else:  # ScrapeScreen.MEDIOAMBIENTE == self:
                return "B6_mapa_todas_variables"
        else:
            if ScrapeScreen.DEMOGRAFIA == self:
                return "B1_mapa_ccaa_todas_variables"
            elif ScrapeScreen.MEDIOFISICO == self:
                return "B2_mapa_ccaa_todas_variables"
            elif ScrapeScreen.ECONOMIA == self:
                return "B3_mapa_ccaa_todas_var"
            elif ScrapeScreen.SERVICIOS == self:
                return "B4_percentiles_map_ccaa_op3"
            elif ScrapeScreen.VIVIENDA == self:
                return "B5_var1_CCAA"
            else:  # ScrapeScreen.MEDIOAMBIENTE == self:
                return "B6_var_CCAA"

    def get_column_names(self, provincia: bool) -> ColumnNames:
        if provincia:
            if ScrapeScreen.DEMOGRAFIA == self:
                return {
                    #"label": "MIN(cp_label_sheet_municipio_provincia)-alias",
                    #"municipio": "cc_Municpio_name_after_set-alias",
                    #"municipio2": "ATTR(cc_label_provincia_muncipio_map_ccaa)-alias",
                    "label": "MÍN.(cp_label__sheets_maps)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "ATRIB(cc_Municpio_name_after_set)-alias",
                }
            elif ScrapeScreen.MEDIOFISICO == self:
                return {
                    "label": "MÍN.(cp_B2_label_sheets_maps)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "cc_Municpio_name_after_set-alias-alias",
                }
            elif ScrapeScreen.ECONOMIA == self:
                return {
                    "label": "MÍN.(cp_B3_label__sheets_maps CORREGIR)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "cc_Municpio_name_after_set-alias",
                }
            elif ScrapeScreen.SERVICIOS == self:
                return {
                    "label": "MÍN.(cp_B4_label__sheets_maps)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "cc_Municpio_name_after_set-alias",
                }
            elif ScrapeScreen.VIVIENDA == self:
                return {
                    "label": "MÍN.(cp_B5_label__sheets_maps)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "cc_Municpio_name_after_set-alias",
                }
            else:  # ScrapeScreen.MEDIOAMBIENTE == self:
                return {
                    "label": "MÍN.(cp_B6_label_sheet_maps)-alias",
                    "municipio": "cc_Municpio_name_after_set-alias",
                    "municipio2": "cc_Municpio_name_after_set-alias",
                }
        else:
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
                    "label": "MIN(cp_B6_label_sheet_maps)-alias",
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
        elif ScrapeTab.B1_DEMOGRAFICO_PROVINCIAL == self:
            return ScrapeScreen.DEMOGRAFIA
        elif ScrapeTab.B2_GEOGRAFICO_PROVINCIAL == self:
            return ScrapeScreen.MEDIOFISICO
        elif ScrapeTab.B3_ECONOMICO_PROVINCIAL == self:
            return ScrapeScreen.ECONOMIA
        elif ScrapeTab.B4_SERVICIOS_PROVINCIAL == self:
            return ScrapeScreen.SERVICIOS
        elif ScrapeTab.B5_VIVIENDA_PROVINCIAL == self:
            return ScrapeScreen.VIVIENDA
        elif ScrapeTab.B6_MEDIOAMBIENTAL_PROVINCIAL == self:
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
        self.modo_provincia: bool = False
        self.current_provincia: Optional[str] = None
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
        self._reset_last_responses()
        await self.page.add_init_script(INIT_SCRIPT)
        await self._switch_to_ccaa()
        await self._close_cookies()
        self.logger.info("Cookies closed.")

    async def finalize(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def scrape(self, pantalla_comunidad: db.PantallaComunidad, provincia: Optional[db.Provincia] = None):
        if self.modo_provincia and provincia is None:
            await self._switch_to_ccaa()
        elif (not self.modo_provincia) and provincia is not None:
            await self._switch_to_provincia()

        current_screen = await self.get_current_screen()
        if current_screen is None:
            raise ScrapeError("Unknown screen found in page")

        self.logger.info(f"Current screen -> {current_screen}")
        requested_screen = ScrapeScreen.from_string(pantalla_comunidad.pantalla.nombre)
        if requested_screen is None:
            raise ScrapeError(f"Unkown requested screen {pantalla_comunidad.pantalla.nombre}")

        self.current_screen = requested_screen.value
        self.current_ccaa = pantalla_comunidad.comunidad.nombre
        self.current_provincia = provincia.nombre if provincia else None

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
        if provincia is None:
            provincia = pantalla_comunidad.comunidad.get_provincia_capital()
            if provincia is None:
                raise ScrapeError(f"Cant find capital for comunidad {pantalla_comunidad.comunidad.nombre}")

        if municipio_code != provincia.codigo:
            self.logger.info(f"Current municipio {municipio_code} is not the expected {provincia.codigo}")
            await self._move_to_provincia(provincia.codigo)
            await self._wait_for_response([ScrapeResponse.CATEGORICAL])

        variable_list = await self._get_all_variables()
        while True:
            current_variable = await self._get_current_variable()
            self.logger.info(f'Processing variable {current_variable}')
            await self._proccess_variable(requested_screen, pantalla_comunidad)
            if current_variable not in variable_list:
                raise ScrapeError(f"No se ha encontrado la variable {current_variable} en la lista {variable_list}")

            self.logger.info(f'Pending variable {variable_list}')
            variable_list.remove(current_variable)
            if len(variable_list) == 0:
                break

            await self._select_variable(variable_list[0])
            # self._reset_last_responses()
            await self._wait_for_response([ScrapeResponse.SET_PARAM])

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
                    if self.modo_provincia:
                        filename = self.cache_path / f'{self.current_ccaa}-{self.current_provincia}-{self.current_screen}-{response["tipo"]}.json'
                    else:
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

    def _reset_all_responses(self):
        self.last_responses_found = []

    async def _move_to_screen(self, screen: ScrapeScreen):
        scrape_tab = screen.to_scrape_tab(self.modo_provincia)
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

    async def _get_select_variables_node(self) -> Locator:
        iframe = self._get_iframe_locator()
        selector = iframe.locator(
            'div.tabComboBoxMenu[role=menu][aria-label^="Inicia la navegación seleccionando una variable de este Bloque"]'
            ' div.tabMenuContent div.tabMenuItem span.tabMenuItemName,'
            'div.tabComboBoxMenu[role=menu][aria-label^="Inicia la navegación seleccionando una variable del Bloque de este Bloque"]'
            ' div.tabMenuContent div.tabMenuItem span.tabMenuItemName,'
            'div.tabComboBoxMenu[role=menu][aria-label^="Inicia la navegación seleccionando una variable del este Bloque"]'
            ' div.tabMenuContent div.tabMenuItem span.tabMenuItemName'
        )
        await selector.first.wait_for(timeout=5000)
        return selector

    async def _get_all_variables(self) -> List[str]:
        await self._click_on_variable()
        selector = await self._get_select_variables_node()
        raw_texts = await selector.all_text_contents()

        # Limpia (strip) cada uno
        clean_texts = [t.strip() for t in raw_texts]
        self.logger.info(f'All variables read {clean_texts}')

        iframe = self._get_iframe_locator()
        try:
            selector = iframe.locator('div.tab-glass.clear-glass.tab-widget')
            await selector.first.wait_for(timeout=5000)
            await selector.click()
        except Exception as ex:
            # self.logger.error(f'Error intentando cancelar la lista {ex}')
            current_variable = await self._get_current_variable()
            selector = await self._get_select_variables_node()
            item = selector.filter(has_text=current_variable).first
            item_count = await item.count()
            if item_count == 0:
                raise ScrapeError(f'La variable {current_variable} no se encuentra en la lista para anular la seleccion???')

            await item.scroll_into_view_if_needed()
            await item.click()

        return clean_texts

    async def _select_variable_node(self) -> Locator:
        iframe = self._get_iframe_locator()
        selector = iframe.locator(
            'div.ParameterControl h3[title^="Inicia la navegación seleccionando una variable de este Bloque"],'
            'div.ParameterControl h3[title^="Inicia la navegación seleccionando una variable del Bloque de este Bloque"],'
            'div.ParameterControl h3[title^="Inicia la navegación seleccionando una variable del este Bloque"]'
        )

        await selector.first.wait_for(timeout=5000)

        parameter_box_selector = selector.locator('xpath=ancestor::div[contains(@class, "ParameterControlBox")]')
        await parameter_box_selector.first.wait_for(timeout=5000)

        variable_selector = parameter_box_selector.locator(
            'div.PCContent div.tabComboBoxNameContainer span.tabComboBoxName'
        )

        await variable_selector.first.wait_for(timeout=5000)

        return variable_selector.first

    async def _click_on_variable(self):
        variable_selector = await self._select_variable_node()
        await variable_selector.first.click()

    async def _select_variable(self, variable: str):
        await self._click_on_variable()
        selector = await self._get_select_variables_node()
        item = selector.filter(has_text=variable).first
        item_count = await item.count()
        if item_count == 0:
            raise ScrapeError(f'La variable {variable} no se encuentra en la lista???')

        await item.scroll_into_view_if_needed()
        await item.click()

        variable_selector = await self._select_variable_node()
        await variable_selector.filter(has_text=variable).first.wait_for(state="visible", timeout=5000)

    async def _get_current_variable(self) -> str:
        variable_selector = await self._select_variable_node()
        return await variable_selector.first.text_content()

    async def _proccess_variable(self, screen: ScrapeScreen, pantalla_comunidad: db.PantallaComunidad):
        current_variable = await self._get_current_variable()
        ts = TableauScraper2(logLevel=logging.ERROR)
        if len(self.last_responses_found) == 0:
            raise ScrapeError(f'No responses got for tableau')

        self.logger.info(f"Loading {self.last_responses_found[0]['tipo']} into tableau TS")
        initial_responses = [response for response in self.last_responses_found
            if response['tipo'] == ScrapeResponse.INITIAL]
        if len(initial_responses) == 0:
            raise ScrapeError(f"No se ha recibido ninguna response del tipo {ScrapeResponse.INITIAL}")

        ts.loads2(initial_responses[0]['response'])
        workbook = ts.getWorkbook()
        # print(ts.parameters)
        not_initial_responses = [response for response in self.last_responses_found
            if response['tipo'] != ScrapeResponse.INITIAL]
        for response in not_initial_responses:
            self.logger.info(f"Loading {response['tipo']} into tableau TS")
            r = json.loads(response['response'])
            print(response['tipo'])
            workbook.updateFullData(r)
            new_workbook = dashboard.getWorksheetsCmdResponse(ts, r)
            if len(new_workbook.worksheets) > 0 or response['tipo'] != ScrapeResponse.FIRST_RENDER:
                workbook = new_workbook

        if len(workbook.worksheets) == 0:
            raise ScrapeNoWorksheetsAfterLoad(f'No se han encontrado worksheets')

        variable_processed = False
        for t in workbook.worksheets:
            if t.name == screen.get_sheet_name(self.modo_provincia):
                if len(t.getColumns()) == 0:
                    raise ScrapeNoWorksheetsAfterLoad(f'El worksheet configurado no tiene campos')

                self._print_ws_info(t, False, screen.get_column_names(self.modo_provincia))
                self._save_ws_info(pantalla_comunidad, current_variable, t, screen.get_column_names(self.modo_provincia))
                variable_processed = True
            # else:
            #    self._save_ws_info(t, True)

        if not variable_processed:
            for t in workbook.worksheets:
                self.logger.info(f"--->{t.name}")
                if t.name == screen.get_sheet_name(self.modo_provincia):
                    self._print_ws_info(t, False)
            raise ScrapeNoVariableProcessed(f'No se ha podido procesar la variable {current_variable}')

    @classmethod
    def _print_ws_info(cls, ws: TableauWorksheet, print_data: bool = False, attrs: Optional[ColumnNames] = None):
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

            sys.stdout.flush()

    @classmethod
    def _save_ws_info(
            cls,
            pantalla_comunidad: db.PantallaComunidad,
            variable: str,
            ws: TableauWorksheet,
            attrs: Optional[ColumnNames] = None
    ):
        for _, row in ws.data.iterrows():
            label = row[attrs.get("label")]
            municipio = fix_mojibake(row[attrs.get('municipio')])
            db.update_or_create_pantalla_comunidad_data(
                pantalla_comunidad.pantalla,
                pantalla_comunidad.comunidad,
                municipio,
                variable,
                label
            )

        db.session.commit()

    async def _switch_to_ccaa(self):
        self._reset_all_responses()
        self.current_ccaa = "País Vasco"
        self.current_screen = ScrapeScreen.DEMOGRAFIA.value
        self.modo_provincia = False
        self.logger.info("Go to tableau main page CCAA .")
        await self.page.goto(
            "https://public.tableau.com/app/profile/reto.demografico/viz/SistemaIntegradodeDatosMunicipales2023/B1_Demogrfico_CCAA"
        )
        await self._wait_for_response([ScrapeResponse.INITIAL, ScrapeResponse.FIRST_RENDER])
        self.logger.info("Initial responses ready!.")

    async def _switch_to_provincia(self):
        self._reset_all_responses()
        self.current_ccaa = "País Vasco"
        self.current_provincia = "Araba"
        self.current_screen = ScrapeScreen.DEMOGRAFIA.value
        self.modo_provincia = True
        self.logger.info("Go to tableau main page provincia .")
        await self.page.goto(
            "https://public.tableau.com/app/profile/reto.demografico/viz/SistemaIntegradodeDatosMunicipales2023/B1_Demogrfico_Provincial"
        )
        await self._wait_for_response([ScrapeResponse.INITIAL, ScrapeResponse.FIRST_RENDER])
        self.logger.info("Initial responses ready!.")


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