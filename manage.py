import click
from ocems_tracker import scraper
from ocems_tracker.industry import Industry
import pandas as pd
import shutil

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

if __name__ == "__main__":
    app()