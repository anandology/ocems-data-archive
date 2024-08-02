"""
Script to scrape data from OCEMS.

https://rtdms.cpcb.gov.in/data/
"""
from bs4 import BeautifulSoup
import requests
from niftyhacks.cache import DiskCache
from dataclasses import dataclass, field

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

    @cache.memoize("state-{state_id}-cities.json")
    def get_cities(self, state_id):
        url = f"https://rtdms.cpcb.gov.in/api/getAllCity/{state_id}"
        return self.session.get(url).json()

    @cache.memoize("industries-{state_id}-{city}.json")
    def get_industries(self, state_id, city):
        url = f"https://rtdms.cpcb.gov.in/api/industryList/45/{state_id}/{city}"
        return self.session.get(url).json()

    @cache.memoize("industries.json")
    def get_all_industries(self):
        for city in self.get_all_cities():
            yield from self.get_industries(city['id'], city['city'])

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

