from datetime import datetime
import logging


def str_to_datetime(date_text):
    if not date_text:
        return None
    else:
        return datetime.strptime(str(date_text), '%Y-%m-%d %H:%M:%S')


class Company:
    def __init__(self, data):
        if data.get('company'):
            self.companyId = data.get('company', {}).get('companyId')
            self.name = data.get('company', {}).get('companyName')
            self.logoSmall = data.get('company', {}).get('logoSmall')
            self.updatedAt = datetime.now()


class Deal:
    def __init__(self, data):
        self.cardId = data.get('cardId')
        self.url = data.get('url')
        # self.condition =
        self.updatedAt = datetime.now()
        self.status = data.get('status')
        self.cardType = data.get('cardType')
        self.cardSubType = data.get('cardSubType')
        if data.get('data', {}):
            self.roomConfiguration = data.get('data', {}).get('roomConfiguration')
            self.buildYear = data.get('data', {}).get('buildYear')
            self.priceText = self._decode_text(data.get('data', {}).get('price'))
            self.price, self.priceCurrency = self._get_price()
            # self.priceCycle = - for Rent
            self.sizeText = data.get('data', {}).get('size')
            self.size, self.sizeUnit = self._get_size()
            self.pricePerSquareMeter = self._get_price_per_square_meter()
            self.description = data.get('data', {}).get('description')
            self.rooms = data.get('data', {}).get('rooms')
            self.sizeLot = data.get('data', {}).get('sizeLot')
            self.sizeMin = data.get('data', {}).get('sizeMin')
            self.sizeMax = data.get('data', {}).get('sizeMax')
            self.newDevelopment = data.get('data', {}).get('newDevelopment')
            self.isOnlineOffer = data.get('data', {}).get('isOnlineOffer')
            self.extraVisibility = data.get('data', {}).get('extraVisibility')
            self.visits = data.get('data', {}).get('visits')
            self.visitsWeekly = data.get('data', {}).get('visitsWeekly')
        if data.get('location', {}):
            self.district = data.get('location', {}).get('district')
            self.city = data.get('location', {}).get('city')
            self.country = data.get('location', {}).get('country')
            self.address = data.get('location', {}).get('address')
            self.latitude = data.get('location', {}).get('latitude')
            self.longitude = data.get('location', {}).get('longitude')
        if data.get('meta', {}):
            self.published = str_to_datetime(data.get('meta', {}).get('published'))
            self.contractType = data.get('meta', {}).get('contractType')
            self.listingType = data.get('meta', {}).get('listingType')
            self.sellStatus = data.get('meta', {}).get('sellStatus')
            self.priceChanged = str_to_datetime(data.get('meta', {}).get('priceChanged'))
        medias = data.get('medias', [])
        if medias:
            self.image1 = medias[0].get('imageLargeJPEG')
            if len(medias) > 1:
                self.image2 = medias[1].get('imageLargeJPEG')
            if len(medias) > 2:
                self.image3 = medias[2].get('imageLargeJPEG')
            if len(medias) > 3:
                self.image4 = medias[3].get('imageLargeJPEG')
        if data.get('company'):
            self.companyId = data.get('company', {}).get('companyId')


    def _decode_text(self, text):
        if not text:
            return None
        else:
            text = text.encode("utf-8").decode("utf-8").strip()
            text = " ".join(text.split())
            return text


    def _get_price(self):
        if not self.priceText:
            return None, None
        else:
            currency = self.priceText.split()[-1]
            price = "".join(self.priceText.split()[:-1])
            try:
                price = float(price)
            except ValueError:
                price = None
            return price, currency
    

    def _get_size(self):
        sizeText = self.sizeText
        if not sizeText:
            return None, None
        else:
            size_split = sizeText.split()
            size = "".join(size_split[:-1]).replace(",", ".")
            try:
                size = float(size)
            except ValueError:
                size = None
            size_unit = size_split[-1]
            return size, size_unit

    def _get_price_per_square_meter(self):
        if self.price and self.size:
            if "m²" not in self.sizeUnit:
                logging.warning("Size unit is not m², check deal with cardId: {}".format(self.cardId))
                return None
            price_per_square_meter = self.price / self.size
            rounded_price_per_square_meter = round(price_per_square_meter, 2)
            return rounded_price_per_square_meter
        else:
            return None
        

class Rent:
    def __init__(self, data):
        self.cardId = data.get('cardId')
        self.url = data.get('url')
        self.updatedAt = datetime.now()
        self.status = data.get('status')
        self.cardType = data.get('cardType')
        self.cardSubType = data.get('cardSubType')
        if data.get('data', {}):
            self.roomConfiguration = data.get('data', {}).get('roomConfiguration')
            self.buildYear = data.get('data', {}).get('buildYear')
            self.priceText = self._decode_text(data.get('data', {}).get('price'))
            self.price, self.priceCurrency, self.priceCycle = self._get_price()
            self.sizeText = data.get('data', {}).get('size')
            self.size, self.sizeUnit = self._get_size()
            self.revenuePerSquareMeter = self._get_revenue_per_square_meter()
            self.description = data.get('data', {}).get('description')
            self.rooms = data.get('data', {}).get('rooms')
            self.sizeLot = data.get('data', {}).get('sizeLot')
            self.sizeMin = data.get('data', {}).get('sizeMin')
            self.sizeMax = data.get('data', {}).get('sizeMax')
            self.newDevelopment = data.get('data', {}).get('newDevelopment')
            self.isOnlineOffer = data.get('data', {}).get('isOnlineOffer')
            self.extraVisibility = data.get('data', {}).get('extraVisibility')
            self.visits = data.get('data', {}).get('visits')
            self.visitsWeekly = data.get('data', {}).get('visitsWeekly')
        if data.get('location', {}):
            self.district = data.get('location', {}).get('district')
            self.city = data.get('location', {}).get('city')
            self.country = data.get('location', {}).get('country')
            self.address = data.get('location', {}).get('address')
            self.latitude = data.get('location', {}).get('latitude')
            self.longitude = data.get('location', {}).get('longitude')
        if data.get('meta', {}):
            self.published = str_to_datetime(data.get('meta', {}).get('published'))
            self.contractType = data.get('meta', {}).get('contractType')
            self.listingType = data.get('meta', {}).get('listingType')
            self.sellStatus = data.get('meta', {}).get('sellStatus')
            self.priceChanged = str_to_datetime(data.get('meta', {}).get('priceChanged'))
        medias = data.get('medias', [])
        if medias:
            self.image1 = medias[0].get('imageLargeJPEG')
            if len(medias) > 1:
                self.image2 = medias[1].get('imageLargeJPEG')
            if len(medias) > 2:
                self.image3 = medias[2].get('imageLargeJPEG')
            if len(medias) > 3:
                self.image4 = medias[3].get('imageLargeJPEG')
        if data.get('company'):
            self.companyId = data.get('company', {}).get('companyId')


    def _decode_text(self, text):
        if not text:
            return None
        else:
            text = text.encode("utf-8").decode("utf-8").strip()
            text = " ".join(text.split())
            return text


    def _get_price(self):
        if not self.priceText:
            return None, None, None
        else:
            priceCycle = self.priceText.split()[-1]
            currency = self.priceText.split()[-3]
            price = "".join(self.priceText.split()[:-3])
            try:
                price = float(price)
            except ValueError:
                price = None
            return price, currency, priceCycle
    

    def _get_size(self):
        sizeText = self.sizeText
        if not sizeText:
            return None, None
        else:
            size_split = sizeText.split()
            size = "".join(size_split[:-1]).replace(",", ".")
            try:
                size = float(size)
            except ValueError:
                size = None
            size_unit = size_split[-1]
            return size, size_unit

    def _get_revenue_per_square_meter(self):
        if self.price and self.size:
            if "m²" not in self.sizeUnit:
                logging.warning("Size unit is not m², check deal with cardId: {}".format(self.cardId))
                return None
            price_per_square_meter = self.price / self.size
            rounded_price_per_square_meter = round(price_per_square_meter, 2)
            return rounded_price_per_square_meter
        else:
            return None
        

class CardDetails:
    def __init__(self, data):
        self.fullAddress = data.get('fullAddress')
        self.postalCode = data.get('postalCode')
        self.upcomingRenovations = data.get('upcomingRenovations')
        self.doneRenovations = data.get('doneRenovations')
        self.conditionType = data.get('conditionType')
        self.housingType = data.get('housingType')
        self.landOwnership = data.get('landOwnership')
        self.debtFreePriceText = data.get('debtFreePriceText')
        self.sellingPriceText = data.get('sellingPriceText')
        self.pricePerSquareMeterText = data.get('pricePerSquareMeterText')
        self.debtShareText = data.get('debtShareText')
        self.treatmentFeeText = data.get('treatmentFeeText')
        self.capitalConsidersationText = data.get('capitalConsidersationText')
        self.totalCompanyConsidersationText = data.get('totalCompanyConsidersationText')
        self.waterFeeText = data.get('waterFeeText')
        self.saunaCostsText = data.get('saunaCostsText')
        self.waterCostsAdditionalText = data.get('waterCostsAdditionalText')
        self.otherCostsText = data.get('otherCostsText')

