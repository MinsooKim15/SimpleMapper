# coding: utf-8
from sqlalchemy import Column, DateTime, Float, String, Text
from sqlalchemy.dialects.mysql import INTEGER, LONGTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class RawFlightAgent(Base):
    __tablename__ = 'RawFlightAgents'

    rawFlightAgentsId = Column(String(100), primary_key=True)
    agentsId = Column(INTEGER(11))
    carrierImageUrl = Column(String(100))
    writeDate = Column(DateTime)
    apiCallId = Column(String(100))


class RawFlightCarrier(Base):
    __tablename__ = 'RawFlightCarriers'

    rawFlightCarriersId = Column(String(100), primary_key=True)
    carrierId = Column(INTEGER(11))
    carrierCode = Column(String(100))
    carrierName = Column(String(100))
    carrierImageUrl = Column(String(100))
    writeDate = Column(DateTime)
    apiCallId = Column(String(100))


class RawFlightItinerary(Base):
    __tablename__ = 'RawFlightItineraries'

    rawFlightItinerariesId = Column(String(100), primary_key=True)
    outboundLegId = Column(String(100))
    inboundLegId = Column(String(100))
    pricingOptions = Column(Text)
    bookingDetailsLink = Column(Text)
    apiCallId = Column(String(100))
    queryAdults = Column(INTEGER(11))
    queryChildren = Column(INTEGER(11))
    queryOriginPlace = Column(String(100))
    queryDestinationPlace = Column(String(100))
    queryOutboundDate = Column(DateTime)
    queryInboundDate = Column(DateTime)
    queryCabinClass = Column(String(100))
    queryGroupingPricing = Column(TINYINT(1))
    writeDate = Column(DateTime)


class RawFlightLeg(Base):
    __tablename__ = 'RawFlightLegs'

    rawFlightLegsId = Column(String(100), primary_key=True)
    legId = Column(String(100))
    segmentsIds = Column(Text)
    originStation = Column(String(100))
    destinationStation = Column(String(100))
    departureDatetime = Column(DateTime)
    arrivalDatetime = Column(DateTime)
    duration = Column(INTEGER(11))
    journeyMode = Column(String(100))
    stops = Column(Text)
    carriers = Column(Text)
    operatingCarriers = Column(Text)
    directionality = Column(String(100))
    flightNumbers = Column(Text)
    writeDate = Column(DateTime)
    apiCallId = Column(String(100))


class RawFlightPlace(Base):
    __tablename__ = 'RawFlightPlaces'

    rawFlightPlacesId = Column(String(100), primary_key=True)
    skyscannerPlaceId = Column(INTEGER(11))
    skyscannerPlaceParentId = Column(INTEGER(11))
    skyscannerPlaceCode = Column(String(100))
    skyscannerPlaceType = Column(String(100))
    skyscannerPlaceName = Column(String(100))
    apiCallId = Column(String(100))
    writeDate = Column(DateTime)


class RawFlightSegment(Base):
    __tablename__ = 'RawFlightSegments'

    rawFlightSegmentsId = Column(String(100), primary_key=True)
    segId = Column(String(100))
    originStation = Column(String(100))
    destinationStation = Column(String(100))
    departureDatetime = Column(DateTime)
    arrivalDatetime = Column(DateTime)
    carrier = Column(String(100))
    operatingCarrier = Column(String(100))
    duration = Column(INTEGER(11))
    flightNumber = Column(String(100))
    journeyMode = Column(String(100))
    directionality = Column(String(100))
    writeDate = Column(DateTime)
    apiCallId = Column(String(100))


class Exchange(Base):
    __tablename__ = 'exchange'

    exchangeId = Column(String(100), primary_key=True)
    currencyName = Column(String(100))
    currencyUnit = Column(String(100))
    todayRate = Column(INTEGER(11))
    weekAgoRate = Column(INTEGER(11))
    monthAgoRate = Column(INTEGER(11))
    rateList = Column(Text)
    rateTitle = Column(Text)
    rateDescription = Column(LONGTEXT)
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)


class FlightPrice(Base):
    __tablename__ = 'flightPrice'

    flightPriceId = Column(String(100), primary_key=True)
    placeId = Column(String(100))
    flightPriceTitle = Column(Text)
    flightPriceDescription = Column(LONGTEXT)
    flightMonthAgoAverage = Column(INTEGER(11))
    flightMonthAgoMinumum = Column(INTEGER(11))
    flightTodayAverage = Column(INTEGER(11))
    flightTodayMinimum = Column(INTEGER(11))
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)


class HotelPrice(Base):
    __tablename__ = 'hotelPrice'

    hotelPriceId = Column(String(100), primary_key=True)
    placeId = Column(String(100))
    hotelPriceTitle = Column(Text)
    hotelPriceDescription = Column(LONGTEXT)
    generalAverage = Column(INTEGER(11))
    luxuryAverage = Column(INTEGER(11))
    hostelAverage = Column(INTEGER(11))
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)


class PlaceMainDesc(Base):
    __tablename__ = 'placeMainDesc'

    placeMainDescId = Column(String(100), primary_key=True)
    score = Column(INTEGER(11))
    placeId = Column(String(100))
    placeMainDescription = Column(LONGTEXT)
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)


class PlaceOp(Base):
    __tablename__ = 'placeOp'

    placeOpId = Column(String(100), primary_key=True)
    placeId = Column(String(100))
    opMsgTitle = Column(Text)
    opMsgDesc = Column(LONGTEXT)
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)


class PlaceStatic(Base):
    __tablename__ = 'placeStatic'

    placeId = Column(String(100), primary_key=True)
    countryName = Column(String(100))
    currencyName = Column(String(100))
    subtitle = Column(Text)
    titleKor = Column(String(100))
    titleEng = Column(String(100))
    created = Column(DateTime, nullable=False)


class RawExchange(Base):
    __tablename__ = 'rawExchange'

    rawExchangeId = Column(String(100), primary_key=True)
    result = Column(INTEGER(11))
    currencyUnit = Column(String(100))
    currencyName = Column(String(100))
    transferBuying = Column(String(100))
    transferSelling = Column(String(100))
    basicRate = Column(String(100))
    bookRate = Column(String(100))
    yearRate = Column(String(100))
    tenDaysRate = Column(String(100))
    korBasicRate = Column(String(100))
    korBookRate = Column(String(100))
    writeDate = Column(DateTime)
    rateDate = Column(DateTime)


class RawWeatherSeoulOnly(Base):
    __tablename__ = 'rawWeatherSeoulOnly'

    rawWeatherSeoulId = Column(String(100), primary_key=True)
    temperature = Column(Float)
    temperatureMax = Column(Float)
    skyCode = Column(INTEGER(11))
    waterfallCode = Column(INTEGER(11))
    waterfallKor = Column(String(100))
    waterfallEng = Column(String(100))
    waterfallProb = Column(Float)
    windSpeed = Column(INTEGER(11))
    windDirection = Column(INTEGER(11))
    windDirectionKor = Column(String(100))
    windDirectionEng = Column(String(100))
    humidity = Column(Float)
    writeDate = Column(DateTime)
    forcastDate = Column(DateTime)


class Weather(Base):
    __tablename__ = 'weather'

    weatherId = Column(String(100), primary_key=True)
    placeId = Column(String(100))
    weatherTitle = Column(Text)
    weatherDescription = Column(LONGTEXT)
    averageTemp = Column(INTEGER(11))
    seoulTemp = Column(INTEGER(11))
    rainDays = Column(INTEGER(11))
    created = Column(DateTime, nullable=False)
    dateToShow = Column(DateTime, nullable=False)