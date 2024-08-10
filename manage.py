import click
from ocems_tracker import scraper
from ocems_tracker.industry import Industry
import pandas as pd
from pathlib import Path
import shutil
from niftyhacks.cache import setup_logger
import logging
import csv
import gzip

# Disable urllib3 warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logger = logging.getLogger()


def setup_logger(verbose=False, format="short"):
    """Setup logger to print the logs to stdout.
    """
    level = logging.DEBUG if verbose else logging.INFO

    format = '[%(asctime)s] %(message)s'
    datefmt = '%Y-%d-%m %H:%M:%S'

    logging.basicConfig(
        level=level,
        format=format,
        datefmt=datefmt)


@click.group()
def app():
    pass

@app.command()
def download_industries():
    api = scraper.API()

    industries = api.get_all_industries()
    data = [Industry.from_dict(ind).to_flat_dict() for ind in industries]
    df = pd.DataFrame(data)
    df.to_csv("data/industries.csv", index=False)


@app.command()
def download_industry_metadata():
    api = scraper.API()
    # this saves the JSONL file in cache
    api.get_all_industry_metadata()
    shutil.copy("cache/industry-metadata/all.jsonl", "data/industry-metadata.jsonl")
    print("generated data/industry-metadata.jsonl")

@app.command()
def download_param_metadata():
    api = scraper.API()
    # this saves the JSONL file in cache
    api.get_all_param_metadata()
    shutil.copy("cache/param-metadata.jsonl", "data/param-metadata.jsonl")
    print("generated data/param-metadata.jsonl")

@app.command
def industry_status():
    """Downloads status of all industries.
    """
    logger.info("Downloading industry status of all industries")
    api = scraper.API()
    data = api.get_all_industry_status()
    date = api.today()
    df = pd.DataFrame(data)
    path = Path(f"daily/status/{date}.csv")
    path.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(path, index=False)
    logger.info("saved the industry status to %s", path)

@app.command()
def live_data():
    """Fetch live parameter values for all industries for yesterday.
    """
    api = scraper.API()
    live = scraper.LiveDataScrapper(api)

    data = live.get_all_live_data()
    df = pd.DataFrame(data)
    df.to_csv("live-data.csv", index=False)

def split_data_by_year(path):
    """Splits a csv.gz data file for an industry by year.
    """
    path = Path(path)
    print("splitting", path)

    root = Path(__file__).parent.resolve() / "archive"

    files = {}
    def get_file(year):
        year = str(year)
        if year not in files:
            new_path = root / year / (path.name + ".tmp")
            new_path.parent.mkdir(parents=True, exist_ok=True)
            files[year] = gzip.open(new_path, "wt")
            files[year].write(header)
        return files[year]

    with gzip.open(path, "rt") as f:
        header = f.readline()

        for line in f:
            t = line.split(",")[-2] # eg: 2016-01-01 01:30:00:000
            year = t.split("-")[0]
            get_file(year).write(line)

    for f in files.values():
        f.close()
        f2 = Path(f.name).with_suffix("")
        shutil.move(f.name, f2.name)
        print("saved", f2.name)

@app.command
def archive():
    """Archive the historical data to internet archive item ocems-data-archive.
    """
    for p in sorted(Path("cache/history/").glob("*.csv.gz"), key=lambda p: int(p.name.split(".")[0])):
        split_data_by_year(p)

@app.command
def historical_data():
    api = scraper.API()
    live = scraper.LiveDataScrapper(api)
    industry_ids = [int(line) for line in open("active.txt")]

    logger.info("Starting download of historical data")
    logger.info("Found %d industries", len(industry_ids))

    for industry_id in industry_ids:
        download_historical_data(live, industry_id)
        split_data_by_year(f"cache/history/{industry_id}.csv.gz")

    logger.info("Download of historical data is complete")

def download_historical_data(live, industry_id):
    path = Path(f"cache/history/{industry_id}.csv.gz")
    path.parent.mkdir(exist_ok=True, parents=True)
    if path.exists():
        logger.info("Data already downloaded for industry %s", industry_id)
        return

    logger.info("Downloading historical data for industry %s", industry_id)

    path2 = path.with_suffix(".gz.tmp")

    try:
        data = live.get_historical_data(industry_id)
        columns = "industry_id station_id device_id param_key param_label time value".split()
        rows = ({k: str(v) for k, v in d.items()} for d in data)

        with gzip.open(path2, "wt") as f:
            w = csv.DictWriter(f, columns)
            w.writeheader()
            w.writerows(rows)

        # df = pd.DataFrame(data)
        # df.to_csv(path, index=False)
        shutil.move(path2, path)
        logger.info("saved %s", path)
    finally:
        if path2.exists():
            logger.info("Deleting partial file %s", path2)
            path2.unlink()

@app.command()
def test():
    import json
    api = scraper.API()
    live = scraper.LiveDataScrapper(api)
    data = live.get_param_values(1035, 31346, 47940, "pm")
    list(data)

if __name__ == "__main__":
    setup_logger()
    app()