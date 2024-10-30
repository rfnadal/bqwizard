import typer
from google.cloud import bigquery
import os
from datetime import datetime

app = typer.Typer(name="bqwizard")
dataset = typer.Typer(name="dataset")
client = bigquery.Client()
table = typer.Typer(name="table")

### Sub Command Groups
app.add_typer(dataset, name="dataset")
app.add_typer(table, name="table")


@dataset.command(name="info")
def get_dataset_info(dataset_id: str):
  dataset = client.get_dataset(dataset_id)
  full_dataset_id = f"{dataset.project}.{dataset.dataset_id}"
  print(f"Here is the info for the {dataset_id} dataset")
  print(f"Description: {dataset.description}")
  print(f"Tables within the dataset: {[table.table_id for table in client.list_tables(dataset)]}")


@table.command(name="info")
def get_table_info(table_id: str):
  table = client.get_table(table_id)
  print(f"Got table '{table.project}.{table.dataset_id}.{table.table_id}'.")
  print(f"Table schema: {table.schema}")
  print(f"Table description: {table.description}")
  print(f"Table has {table.num_rows} rows")
  print(f"Table was last updated {table.modified} rows")

@table.command(name="freshness")
def get_table_freshness(table_id: str):
  table = client.get_table(table_id)
  current_time = datetime.utcnow()
  last_modified_dt = datetime.fromtimestamp(table.modified.timestamp())
  print(f"The {table_id} is currently this old: {current_time - last_modified_dt}")


if __name__ == "__main__":
    app()
