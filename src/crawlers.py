from apify import Actor
import asyncio
import httpx
import random
import re
from bs4 import BeautifulSoup
from src.models import Deal, Rent, Company, CardDetails


SLEEP_TIME = 3

async def request_get_oikotie(params, api_headers, proxy_url=None):
    timeout = httpx.Timeout(30.0, connect=5.0)
    proxies = {
        'http://': proxy_url,
        'https://': proxy_url
    } if proxy_url else None
    client = httpx.AsyncClient(timeout=timeout, proxies=proxies)
    try:
        response = await client.get('https://asunnot.oikotie.fi/api/search', params=params, 
                                    headers=api_headers)
        response.raise_for_status()
        data = response.json()

        cards = data.get('cards', [])
        total_card = data.get('found', 0)

        try:
            total_card = int(total_card)
        except ValueError:
            total_card = 23456
            Actor.log.error("Total card is not an int: %s", total_card)

    except httpx.RequestError as e:
        Actor.log.error("An error occurred while requesting %s; Error: %s", e.request.url, e)
        cards, total_card = [], 0
    except httpx.HTTPStatusError as e:
        Actor.log.error("Error response %s while requesting %s; Error: %s", e.response.status_code, e.request.url, e)
        cards, total_card = [], 0
    except Exception as e:
        Actor.log.error("An unexpected error occurred: %s", e)
        cards, total_card = [], 0
    finally:
        await client.aclose()
    return cards, total_card


async def fetch_cards_with_retries(deal_params, api_headers, offset, 
                                   max_retries=5, proxy_url=None):
    retries = 0
    while retries < max_retries:
        try:
            cards, total_card = await request_get_oikotie({**deal_params, 'offset': offset}, 
                                                          api_headers, proxy_url)
            if cards:
                return cards, total_card
        except Exception as e:
            Actor.log.error(f"Error at offset {offset}: {e}, retrying {retries + 1} times")
        retries += 1
        await asyncio.sleep(5)  # Wait before retrying to avoid hammering the server
    Actor.log.error(f"Failed to fetch data after {max_retries} retries at offset {offset}")
    return None, 0  # Indicate failure to fetch cards


def extract_deal_cards(cards):
    all_deals = [Deal(card).__dict__ for card in cards if card.get('cardId')]
    all_company = [Company(card).__dict__ for card in cards]
    all_company = [company for company in all_company if company.get('companyId')]
    return all_deals, all_company


def extract_rent_cards(cards):
    all_rents = [Rent(card).__dict__ for card in cards if card.get('cardId')]
    all_company = [Company(card).__dict__ for card in cards]
    all_company = [company for company in all_company if company.get('companyId')]
    return all_rents, all_company


async def fetch_card_details(url, proxy_url=None) -> CardDetails:
    result = {}
    proxies = {
        'http://': proxy_url,
        'https://': proxy_url
    } if proxy_url else None

    async with httpx.AsyncClient(proxies=proxies) as client:
        try:
            response = await client.get(url, timeout=30)
        except Exception as e:
            Actor.log.error(f"Error at url {url}: {e}")
            return result

        if response.status_code not in (404, 410, 200):
            Actor.log.error(f"Error at url {url}: {response.status_code}")
            return result
    
    soup = BeautifulSoup(response.text, 'html.parser')
    breadcrumbs = soup.find('div', {'class': 'breadcrumbs'})
    if breadcrumbs:
        try:
            full_address = breadcrumbs.find_all('span', {'class': 'breadcrumbs__item'})[-1].text.strip()
            postal_code = full_address.split(',')[-1].strip()
            postal_code = re.search(r'\d+', postal_code).group()
        except Exception as e:
            full_address = -1
            Actor.log.error(f"Error at url {url}: {e}")
        result['fullAddress'] = full_address
        result['postalCode'] = postal_code

    listing_details_container = soup.find('div', {'class': 'listing-details-container'})

    def clean_value_text(value):
        return value.replace('\xa0', ' ').strip() if value else None

    all_listing_details_divs = listing_details_container.find_all('div', {'class': 'listing-details'}) if listing_details_container else []
    for listing_details_div in all_listing_details_divs:
        listing_details_title = listing_details_div.find('h3', {'class': 'listing-details__title'})
        basic_details = listing_details_div.find_all('div', {'class': 'info-table__row'})
        if not listing_details_title or not basic_details:
            continue
        listing_details_title = listing_details_title.text.strip()
        if listing_details_title == 'Perustiedot': # Basic details
            for row in basic_details:
                title = row.find('dt', {'class': 'info-table__title'})
                value = row.find('dd', {'class': 'info-table__value'})
                if not title or not value:
                    continue
                title = title.text.strip()
                value = clean_value_text(value.text)
                if title == 'Tulevat remontit':
                    result['upcomingRenovations'] = value
                elif title == 'Tehdyt remontit':
                    result['doneRenovations'] = value
                elif title == 'Kunto':
                    result['conditionType'] = value
                elif title == 'Asumistyyppi':
                    result['housingType'] = value
        elif listing_details_title == 'Talon ja tontin tiedot': # Lot and house
            for row in basic_details:
                title = row.find('dt', {'class': 'info-table__title'})
                value = row.find('dd', {'class': 'info-table__value'})
                if not title or not value:
                    continue
                title = title.text.strip()
                value = clean_value_text(value.text)
                if title == 'Tontin omistus':
                    result['landOwnership'] = value
        elif listing_details_title == 'Hinta': # Price
            for row in basic_details:
                title = row.find('dt', {'class': 'info-table__title'})
                value = row.find('dd', {'class': 'info-table__value'})
                if not title or not value:
                    continue
                title = title.text.strip()
                value = clean_value_text(value.text)
                if title == 'Velaton hinta':
                    result['debtFreePriceText'] = value
                elif title == 'Myyntihinta':
                    result['sellingPriceText'] = value
                elif title == 'Neliöhinta':
                    result['pricePerSquareMeterText'] = value
                elif title == 'Velkaosuus':
                    result['debtShareText'] = value
        elif listing_details_title == 'Vastikkeet': # Considersation
            for row in basic_details:
                title = row.find('dt', {'class': 'info-table__title'})
                value = row.find('dd', {'class': 'info-table__value'})
                if not title or not value:
                    continue
                title = title.text.strip()
                value = clean_value_text(value.text)
                if title == 'Hoitovastike':
                    result['treatmentFeeText'] = value
                elif title == 'Pääomavastike':
                    result['capitalConsidersationText'] = value
                elif title == 'Yhtiövastike yhteensä':
                    result['totalCompanyConsidersationText'] = value
        elif listing_details_title == 'Muut maksut': # Other payments
            for row in basic_details:
                title = row.find('dt', {'class': 'info-table__title'})
                value = row.find('dd', {'class': 'info-table__value'})
                if not title or not value:
                    continue
                title = title.text.strip()
                value = clean_value_text(value.text)
                if title == 'Vesimaksu':
                    result['waterFeeText'] = value
                elif title == 'Saunan kustannukset':
                    result['saunaCostsText'] = value
                elif title == 'Vesimaksun lisätiedot':
                    result['waterCostsAdditionalText'] = value
                elif title == 'Muut kustannukset':
                    result['otherCostsText'] = value

    return CardDetails(result)


async def deal_crawler_generator(headers_list, proxy_url=None):
    deal_params = {
        'cardType': '100',
        'buildingType[]': ['1', '256', '2', '64', '4', '8', '32', '128'],
        'lotOwnershipType[]': '1', # land ownership
        'habitationType[]': '1', # housing type
        'roomCount[]': ['1', '2', '3', '4', '5', '6', '7'],
        'limit': '24',
        'offset': '0',
        'sortBy': 'published_sort_desc',
    }
    # For now, not filtering by condition, extracted from the card html data
    # condition_type = {'64': "All", '2': "Good", '4': "Satisfying", '8': "Passable", '32': "New"}
    # for condition, description in condition_type.items():
    # if condition != '64':
    #     deal_params['conditionType[]'] = condition
    total_card = 1
    offset = 0
    continuous_failures = 0
    while offset < total_card:
        api_headers = random.choice(headers_list)
        cards, actual_total_card = await fetch_cards_with_retries(deal_params, api_headers, offset, 
                                                                    proxy_url=proxy_url)
        if not cards:
            continuous_failures += 1
            if continuous_failures > 5:
                Actor.log.error(f"Failed to fetch cards at offset {offset} deal, stopping")
                return
            offset += 24
            continue
        continuous_failures = 0

        if total_card == 1:  # Only update log the first time
            Actor.log.info(f"Starting fetch with {actual_total_card} total cards found.")
        
        total_card = actual_total_card  # Update the total count based on fetched data
        deals, companies = extract_deal_cards(cards)
        
        Actor.log.info(f"Fetched cards at offset {offset}, got {len(deals)} deals and {len(companies)} companies.")
        offset += 24

        yield deals, companies
        await asyncio.sleep(SLEEP_TIME/len(headers_list))


async def rent_crawler_generator(headers_list, proxy_url=None):
    rent_params = {
        'cardType': '101',
        'limit': '24',
        'offset': '0',
        'sortBy': 'published_sort_desc',
    }
        
    total_card = 1
    offset = 0
    continuous_failures = 0
    while offset < total_card:
        api_headers = random.choice(headers_list)
        cards, actual_total_card = await fetch_cards_with_retries(rent_params, api_headers, offset, 
                                                                    proxy_url=proxy_url)
        if not cards:
            continuous_failures += 1
            if continuous_failures > 5:
                Actor.log.error(f"Failed to fetch cards at offset {offset} rents, stopping")
                return
            offset += 24
            continue
        continuous_failures = 0

        if total_card == 1:  # Only update log the first time
            Actor.log.info(f"Starting fetch with {actual_total_card} total cards found.")
        
        total_card = actual_total_card  # Update the total count based on fetched data
        reants, companies = extract_rent_cards(cards)
        
        Actor.log.info(f"Fetched cards at offset {offset}, got {len(reants)} rents and {len(companies)} companies.")
        offset += 24

        yield reants, companies
        await asyncio.sleep(SLEEP_TIME/len(headers_list))
