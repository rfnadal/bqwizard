import typer
from rich import print
from .utils import get_client



table = typer.Typer()




@table.command(name="info")
def get_table_info(table_id: str):
  """ Take in the full qualified table name and return's the associated metadata. """
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
