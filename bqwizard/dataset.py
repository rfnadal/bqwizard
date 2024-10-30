import typer
from rich import print
from .utils import get_client


dataset = typer.Typer()


@dataset.command(name="info")
def get_dataset_info(dataset_id: str):
  client = get_client()
  dataset = client.get_dataset(dataset_id)
  full_dataset_id = f"{dataset.project}.{dataset.dataset_id}"
  print(f"[green]Dataset Info:[/green]")
  print(f"Description: {dataset.description}")
  print(f"Dataset Location: {dataset.location}")
  print(f"Tables within the dataset: {[table.table_id for table in client.list_tables(dataset)]}")