import click
from ocems_tracker import scraper
from ocems_tracker.industry import Industry
import pandas as pd

@click.group()
def app():
    pass

@app.command()
def download_industries():
    print("download industries")
    api = scraper.API()

    industries = api.get_all_industries()
    active_ids = api.get_all_active_industry_ids()

    # for ind in industries:
    #     if ind['id'] in active_ids:
    #         print(ind)
    #         break

    # print("done")
    # return

    data = [Industry.from_dict(ind).to_flat_dict() for ind in industries if ind['id'] in active_ids]
    df = pd.DataFrame(data)
    df.to_csv("data/industries.csv", index=False)


if __name__ == "__main__":
    app()