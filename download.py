import asyncio
from playwright.async_api import async_playwright
import json

def sleep(ms):
    return asyncio.sleep(ms / 1000)

async def check_new_data(page):
    pages_found = []

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

        pages_found.append(response["tipo"])
        filename = f'RESPONSE-{response["tipo"]}.json'
        with open(filename, "w", encoding="utf-8") as file:
            file.write(response["responseText"])
        print(f'Archivo {filename} escrito correctamente.')

    return pages_found

async def main():
    async with async_playwright() as p:
        print(p.chromium.executable_path)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            bypass_csp=True,  # Opcional: Ignorar la política de seguridad de contenido
            ignore_https_errors=True,  # Opcional: Ignorar errores de HTTPS
        )
        page = await context.new_page()

        encontrado = False

        # Escuchar las respuestas de las solicitudes de red
        async def handle_response(response):
            url = response.url
            if "/categorical-filter-by-index" in url:  # Filtra por la URL del AJAX
                print(url)
                try:
                    print(await response.body())
                    print("Respuesta de AJAX:", await response.json())  # Obtén el cuerpo de la respuesta
                except Exception as ex:
                    print(ex)

        page.on("response", handle_response)

        await page.add_init_script(
            """//() => {
                console.log("init");
                var open = window.XMLHttpRequest.prototype.open;
                window.top.__responses = [];

                window.XMLHttpRequest.prototype.open = function (method, url, async, user, pass) {
                    console.log(url);
                    const validUrls = {
                        initial: 'bootstrapSession/sessions/',
                        first_render: '/notify-first-client-render-occurred',
                        set_param: '/set-parameter-value-from-index',
                        new_layout: '/ensure_layout_for_sheet',
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
        )

        await page.goto(
            "https://public.tableau.com/app/profile/reto.demografico/viz/SistemaIntegradodeDatosMunicipales2023/B1_Demogrfico_CCAA"
        )

        wait_for_pages = ["initial", "first_render"]
        while len(wait_for_pages) > 0:
            # await sleep(5000)
            await page.wait_for_timeout(3000)
            pages_found = await check_new_data(page)
            print(wait_for_pages)
            wait_for_pages = [item for item in wait_for_pages if item not in pages_found]
            print(wait_for_pages)

        # Cerrar cookies
        boton = await page.query_selector("#onetrust-accept-btn-handler")
        if boton:
            await boton.click()  # Pulsar el botón
            print("Botón de cookies pulsado correctamente.")
        else:
            print("No se encontró el botón de cookies.")

        # await sleep(5000)
        await page.wait_for_timeout(3000)
        iframe = page.frame_locator('div#embedded-viz-wrapper iframe')
        selector = iframe.locator("#tableau_base_widget_ParameterControl_0 span[role=button]")
        current_selected = await selector.text_content()
        visitados = [current_selected]
        print(current_selected)
        await selector.click()

        span = iframe.locator(f'span.tabMenuItemName:has-text("{current_selected}")')
        try:
            await span.wait_for(state="attached", timeout=5000)  # Esperar hasta 5 segundos
            print(f'El <span> con el texto "{current_selected}" apareció.')
        except Exception as error:
            print(f'El <span> con el texto "{current_selected}" no apareció en el tiempo esperado.')

        padre = await span.evaluate_handle(
            """(elemento) => {
                return elemento.closest('div.tabMenuContent');
            }"""
        )

        textos = await padre.evaluate(
            """(elementoPadre) => {
                const spans = elementoPadre.querySelectorAll('span.tabMenuItemName');
                const resultados = [];
                spans.forEach((span) => {
                    resultados.push(span.innerText);
                });
                return resultados;
            }"""
        )

        print(textos)

        pendientes = [item for item in textos if item not in visitados]

        await page.wait_for_timeout(3000)

        node_clickado = await padre.evaluate_handle(
            """(elementoPadre, texto) => {
                const spans = elementoPadre.querySelectorAll('span.tabMenuItemName');
                const node = Array.from(spans).find((span) => span.innerText.includes(texto));
                if (node) {
                    const parentNode = node.closest('div.tabMenuItemNameArea');
                    if (parentNode) {
                        return parentNode;
                    }
                }
                return null;
            }""",
            pendientes[0],
        )

        if node_clickado:
            print("Clickado")
            await node_clickado.click()
        else:
            print("NO CLICKADO")

        wait_for_pages = ["set_param"]
        while len(wait_for_pages) > 0:
            # await sleep(5000)
            await page.wait_for_timeout(3000)
            if len(pages_found) > 0:
                print(pages_found)
            pages_found = await check_new_data(page)
            print(1, wait_for_pages)
            wait_for_pages = [item for item in wait_for_pages if item not in pages_found]
            print(2, wait_for_pages)

        await browser.close()

# Ejecutar el script
asyncio.run(main())