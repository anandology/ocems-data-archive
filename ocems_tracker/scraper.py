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

cache = DiskCache("cache/")

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

    def get_industry_ids(self):
        industries = self.get_all_industries()
        return sorted(industry['id'] for industry in industries)

    def get_all_active_industry_ids(self) -> set[int]:
        """Returns a set of ids of all active industries.
        """
        active = set()
        for city in self.get_all_cities():
            active |= {d['id'] for d in self._get_active_industries(city['id'], city['city'])}
        return active

    @cache.memoize("industries-active/{state_id}-{city}.json")
    def _get_active_industries(self, state_id, city):
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

    @cache.memoize("device-data/{date}/{station_id:06d}-{device_id:06d}.json")
    def _get_device_data(self, date, station_id, device_id):
        """Downloads the data for last 48 hours. The date is used as the cacke key
        to avoid repeated downloads for the same day.
        """
        url = f"https://rtdms.cpcb.gov.in/api/stations/{station_id}/devices/{device_id}/data"


    def get_device_data(self, station_id, device_id):
        date = self.today()
        return self._get_device_data(date, station_id, device_id)


    def today(self) -> str:
        """Returns Today in timezone IST as a string.
        """
        t = datetime.datetime.utcnow()
        tz = pytz.timezone("Asia/Kolkata")
        return pytz.utc.localize(t, is_dst=None).astimezone(tz)



