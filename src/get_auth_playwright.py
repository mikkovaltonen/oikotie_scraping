import asyncio
from apify import Actor
from playwright.async_api import async_playwright
from random_user_agent.user_agent import UserAgent


async def oikotie_search_get_headers(headless=True, proxy_server=None,
                                     proxy_username=None, proxy_password=None
                                     ) -> dict:
    Actor.log.info("Starting Playwright Profile")
    url = "https://asunnot.oikotie.fi/myytavat-asunnot?pagination=2&cardType=100"
    if proxy_server:
        proxy = {
            'server': proxy_server,
            'username': proxy_username,
            'password': proxy_password
        }
        Actor.log.info(f"Running playwright with proxy: {proxy_server}")
    else:
        proxy = None
        Actor.log.info("Running playwright without proxy")
    user_agent = UserAgent().get_random_user_agent()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless, 
            proxy=proxy,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        )
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()

        all_responses = []        
        def response_callback(response):
            all_responses.append(response)

        page.on("response", response_callback)
        await page.goto(url)
        await page.wait_for_load_state()
        page.remove_listener("response", response_callback)

        await page.close()
        await context.close()
        await browser.close()
        Actor.log.info("Got all responses, stopping Playwright resources")

        for response in all_responses:
            request = response.request
            if "ota-token" in str(request.headers):
                Actor.log.info("Found token")
                return request.headers

    return None


async def setup_api_headers(headless=True,
                            proxy_server=None,
                            proxy_username=None, 
                            proxy_password=None) -> dict:
    """Setup API headers with retries, returns headers or raises an error after maximum retries."""
    retries = 0
    while retries < 5:
        try:
            api_headers = await oikotie_search_get_headers(
                headless=headless,
                proxy_server=proxy_server,
                proxy_username=proxy_username,
                proxy_password=proxy_password
            )
            if api_headers:
                return api_headers
        except Exception as e:
            Actor.log.error(f"Error getting API headers: {e}", exc_info=True)
            await asyncio.sleep(43200)  # Retry after 12 hours
            retries += 1
    raise Exception("Failed to get API headers after 5 retries")