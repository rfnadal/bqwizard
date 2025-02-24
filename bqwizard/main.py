import typer
from google.cloud import bigquery
import os
from datetime import datetime
from rich import print
from bqwizard import dataset
from .utils import get_client


app = typer.Typer(name="bqwizard")


client = bigquery.Client()
table = typer.Typer(name="table")

### Sub Command Groups
app.add_typer(dataset.dataset, name="dataset")
app.add_typer(table.table, name="table")




if __name__ == "__main__":
    app()
