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
app.add_typer(table, name="table")





@table.command(name="info")
def get_table_info(table_id: str):
  client = get_client()
  table = client.get_table(table_id)
  print(f"Got table '{table.project}.{table.dataset_id}.{table.table_id}'.")
  print(f"Table schema: {table.schema}")
  print(f"Table description: {table.description}")
  print(f"Table has {table.num_rows} rows")
  print(f"Table was last updated {table.modified} rows")

@table.command(name="freshness")
def get_table_freshness(table_id: str):
  client = get_client()
  table = client.get_table(table_id)
  current_time = datetime.utcnow()
  last_modified_dt = datetime.fromtimestamp(table.modified.timestamp())
  print(f"The {table_id} is currently this old: {current_time - last_modified_dt}")


if __name__ == "__main__":
    app()
