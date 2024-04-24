"""
This module defines the `main()` coroutine for the Apify Actor, executed from the `__main__.py` file.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

import asyncio
import os
import dotenv
dotenv.load_dotenv()

from apify import Actor

from src.get_auth_playwright import setup_api_headers
from src.crawlers import deal_crawler_generator, rent_crawler_generator, fetch_card_details

# To run this Actor locally, you need to have the Playwright browsers installed.
# Run `playwright install --with-deps` in the Actor's virtual environment to install them.
# When running on the Apify platform, they are already included in the Actor's Docker image.

async def update_deal_details(deal, oikotie_deals_dataset: Actor, 
                                       proxy_url=None):
    deal_url = deal.get('url')
    more_details = await fetch_card_details(deal_url, proxy_url)
    deal.update(more_details.__dict__)
    card_id = str(deal.get('cardId'))
    await oikotie_deals_dataset.set_value(card_id, deal)


async def main() -> None:
    async with Actor:
        proxy_server = os.getenv("PROXY_SERVER")
        proxy_username = os.getenv("PROXY_USERNAME")
        proxy_password = os.getenv("PROXY_PASSWORD")
        proxy_url = f"http://{proxy_username}:{proxy_password}@{proxy_server}"
        if not proxy_server or not proxy_username or not proxy_password:
            Actor.log.error("Proxy not set up, exiting")
            return

        actor_input = await Actor.get_input() or {}
        crawler_mode = actor_input.get('crawler_mode', 'deal')
        Actor.log.info(f"Starting crawler with mode: {crawler_mode}")
        num_workers = actor_input.get('num_workers', 1)

        headers_list = [await setup_api_headers(headless=True, proxy_server=proxy_server, 
                                                proxy_username=proxy_username, 
                                                proxy_password=proxy_password)
                        for _ in range(num_workers)]
        headers_list = [headers for headers in headers_list if headers]

        Actor.log.info(f"Got new api headers, starting crawler with {len(headers_list)} headers")
        if not headers_list:
            Actor.log.error("Failed to get API headers")
            return
        
        if crawler_mode == 'deal':
            oikotie_deals_dataset = await Actor.open_key_value_store(name='oikotie-deals')
            oikotie_companies_dataset = await Actor.open_key_value_store(name='oikotie-companies')

            deal_crawler_generator_task = deal_crawler_generator(headers_list, proxy_url)
            async for deals, companies in deal_crawler_generator_task:
                deal_tasks = [update_deal_details(deal, oikotie_deals_dataset, proxy_url) for deal in deals]
                await asyncio.gather(*deal_tasks)
                for company in companies:
                    company_id = str(company.get('companyId'))
                    await oikotie_companies_dataset.set_value(company_id, company)
        elif crawler_mode == 'rent':
            oikotie_rents_dataset = await Actor.open_key_value_store(name='oikotie-rents')
            oikotie_companies_dataset = await Actor.open_key_value_store(name='oikotie-companies')

            rent_crawler_generator_task = rent_crawler_generator(headers_list, proxy_url)
            async for rents, companies in rent_crawler_generator_task:
                rent_tasks = [update_deal_details(rent, oikotie_rents_dataset) for rent in rents]
                await asyncio.gather(*rent_tasks)
                for company in companies:
                    company_id = str(company.get('companyId'))
                    await oikotie_companies_dataset.set_value(company_id, company)
        else:
            Actor.log.error(f"Invalid crawler mode: {crawler_mode}")

    