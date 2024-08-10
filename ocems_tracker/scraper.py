"""
Script to scrape data from OCEMS.

https://rtdms.cpcb.gov.in/data/
"""
from bs4 import BeautifulSoup
import requests
from niftyhacks.cache import DiskCache
from dataclasses import dataclass, field
import datetime
import pytz
import logging
import json
import time

cache = DiskCache("cache/")

logger = logging.getLogger(__name__)

headers = {
    "Referer": "https://rtdms.cpcb.gov.in/data/",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
}

class API:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.session.verify = False

    @cache.memoize("states.json")
    def get_states(self):
        url = "https://rtdms.cpcb.gov.in/api/getAllState"
        return self.session.get(url).json()

    @cache.memoize("cities/{state_id}.json")
    def get_cities(self, state_id):
        url = f"https://rtdms.cpcb.gov.in/api/getAllCity/{state_id}"
        return self.session.get(url).json()

    @cache.memoize("industries/{state_id}-{city}.json")
    def get_industries(self, state_id, city):
        url = f"https://rtdms.cpcb.gov.in/api/industryList/45/{state_id}/{city}"
        return self.session.get(url).json()

    @cache.memoize("industries.json")
    def get_all_industries(self):
        for city in self.get_all_cities():
            yield from self.get_industries(city['id'], city['city'])

    @cache.memoize("industry-ids.jsonl")
    def get_industry_ids(self):
        industries = self.get_all_industries()
        return sorted(industry['id'] for industry in industries)

    def get_all_industry_status(self):
        industry_ids = self.get_industry_ids()
        d = {row['id']: row['status'] for row in self._get_all_industry_live_status()}

        for industry_id in industry_ids:
            status = d.get(industry_id) or 'offline'
            yield dict(industry_id=industry_id, status=status)

    def _get_all_industry_live_status(self):
        """Returns status of all live industries.

        Returns a generator with one record for every live industry. Each record looks like the following:

            {"id": 2364, "status": "live"}
        """
        for city in self.get_all_cities():
            yield from self._get_industry_live_status(city['id'], city['city'])

    def _get_industry_live_status(self, state_id, city):
        logger.info("fetching live status for city %s - %s", state_id, city)
        url = f"https://rtdms.cpcb.gov.in/api/industryListStatus/45/{state_id}/{city}"
        r = self.session.get(url)
        return r.json()

    @cache.memoize("cities.json")
    def get_all_cities(self):
        for state in self.get_states():
            for city in self.get_cities(state["id"]):
                yield city

    @cache.memoize("industry-metadata/{industry_id}.json")
    def get_industry_metadata(self, industry_id):
        print("get_industry_metadata", industry_id)
        url = f"https://rtdms.cpcb.gov.in/api/industryMapDetailNEW/{industry_id}"
        print("GET", url)
        data = self.session.get(url).json()
        if not data:
            data = {
                "industry": {
                    "id": industry_id
                }
            }
        # remove recentData
        data.pop('recentData', None)
        return self.strip_sensitive_data(data)

    @cache.memoize("industry-metadata/all.jsonl")
    def get_all_industry_metadata(self):
        for id in self.get_industry_ids():
            yield self.get_industry_metadata(id)

    def strip_sensitive_data(self, data):
        """Removes sensitive fields like email, phone number, password and tokens.
        """
        def is_sensitive_key(key):
            key = key.lower()
            sensitive_names = ['email', 'phone', 'contactno', 'password', 'token']
            return any(name in key for name in sensitive_names)

        if isinstance(data, list):
            return [self.strip_sensitive_data(d) for d in data]
        elif isinstance(data, dict):
            return {k: self.strip_sensitive_data(v) for k, v in data.items() if not is_sensitive_key(k)}
        else:
            return data


    def today(self) -> datetime.date:
        """Returns Today in timezone IST as a string.
        """
        t = datetime.datetime.utcnow()
        tz = pytz.timezone("Asia/Kolkata")
        return pytz.utc.localize(t, is_dst=None).astimezone(tz).date()

    def yesterday(self) -> datetime.date:
        """Returns Today in timezone IST as a string.
        """
        date = self.today() - datetime.timedelta(days=1)
        return date.date()

    def get_all_industry_metadata_summary(self):
        metadata = self.get_all_industry_metadata()

    # @cache.memoize("tmp/status/{industry_id}.json")
    # def get_industry_status(self, industry_id) -> str:
    #     """Returns the industry status for an industry.

    #     The possible response are:
    #         - live
    #         - offline
    #         - error (server error in accessing the API)
    #         - NA (No data available, API response is empty)
    #     """
    #     logger.info("get_industry_status %s", industry_id)

    #     url = f"https://rtdms.cpcb.gov.in/api/industryMapDetailNEW/{industry_id}"
    #     r = self.session.get(url)
    #     if r.status_code != 200:
    #         logger.error("response: %s %s", r.status_code, r.reason)
    #         return 'error'
    #     try:
    #         d = r.json()
    #     except Exception:
    #         logger.error("Failed to parse json", exc_info=True)
    #         return 'error'
    #     return d.get('industryStatus') or 'error'

    # def get_all_industry_status(self):
    #     return [{"industry_id": id, "status": self.get_industry_status(id)} for id in self.get_industry_ids()]

    @cache.memoize("param-metadata.jsonl")
    def get_all_param_metadata(self):
        for industry_id in self.get_industry_ids():
            data = self.get_param_metadata(industry_id)
            if data:
                yield data

    def get_param_metadata(self, industry_id):
        def process_station(station_data):
            station = {}
            station['id'] = station_data['id']
            station['name'] = station_data['name']
            station['devices'] = [process_device(station['id'], d) for d in station_data['devices']]
            return station

        def process_device(station_id, device_data):
            device = {}
            device['id'] = device_data['id']
            device['name'] = device_data['name']
            device['params'] = [process_param(station_id, d) for d in device_data['params']]
            return device

        def process_param(station_id, param_data):
            param = {}
            d = param_data['stdParam']
            param['id'] = d['id']
            param['type'] = d['type']
            param['name'] = d['name']
            param['key'] = d['paramKey']
            param['label'] = d['label']
            param['unit'] = d['stdUnit']
            try:
                param['max'] = data['thresholdNEW'][str(station_id)][str(param['id'])]['max']
            except KeyError:
                pass
            return param

        def process_industry(data):
            industry = {}
            industry['id'] = data['industry']['id']
            industry['name'] = data['industry']['name']
            industry['stations'] = [process_station(d) for d in data['stations']]
            return industry

        data = self.get_industry_metadata(industry_id)
        # No data
        if "stations" not in data:
            return None
        return process_industry(data)


class LiveDataScrapper:
    """Utility to download live data for all industries.
    """
    def __init__(self, api):
        self.api = api
        self.session = api.session
        self.param_metadata = api.get_all_param_metadata()
        self.metadata_lookup = {d['id']: d for d in self.param_metadata}
        # fetch last 2 days of data
        self.start_date = "2d-ago"

    def get_historical_data(self, industry_id):
        self.start_date = "10y-ago"
        date = "history"
        return self._get_live_data(date, industry_id)

    def get_live_data(self, industry_id):
        date = self.api.today()
        return self._get_live_data(date, industry_id)

    def get_all_live_data(self):
        for industry_id in self.api.get_industry_ids():
            yield from self.get_live_data(industry_id)

    # @cache.memoize("live-data/{date}/{industry_id}.jsonl")
    def _get_live_data(self, date, industry_id):
        industry_id = int(industry_id)
        if industry_id not in self.metadata_lookup:
            print("Unknown industry_id: %r", industry_id)
            return []
        industry = self.metadata_lookup[industry_id]
        for station in industry['stations']:
            for device in station['devices']:
                for param in device['params']:
                    row = [industry['id'], station['id'], device['id'], param['key'], param['label']]
                    try:
                        data = self.get_param_values(industry_id, station['id'], device['id'], param['key'])

                        data2 = [row + [d['time'], d['value']] for d in data[param['name']]]
                        yield from data2
                    except Exception:
                        args = dict(industry_id=industry['id'],
                                    station_id=station['id'],
                                    device_id=device['id'],
                                    param_key=param['key'])
                        logger.error("FAILED PARAMS %s", json.dumps(args))
                        logger.error("Failed to fetch param values", exc_info=True)

    def get_param_values(self, industry_id, station_id, device_id, param_key):
        logger.info("get_param_values %s %s %s %s", industry_id, station_id, device_id, param_key)
        retries = 3
        r = None
        for i in range(retries):
            try:
                url = f"https://rtdms.cpcb.gov.in/api/stations/{station_id}/devices/{device_id}/data"
                payload = {
                    "avg": "minute_15",
                    "param": param_key,
                    "startDate": self.start_date
                }
                r = self.session.post(url, json=payload)
                return r.json()
            except Exception as e:
                if i+1 < retries:
                    logger.error("Failed with error %s", e)
                    logger.error("Response: %s", r.text if r else "")
                    logger.error("Retrying...")
                    time.sleep(2)
                else:
                    logger.error("Failed even after retries")
                    raise
