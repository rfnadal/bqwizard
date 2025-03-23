import click
from .commands import dataset 

@click.group()
def cli():
    """A Big Query CLI Tool for those in a hurry."""
    pass

cli.add_command(dataset.dataset)

if __name__=="__main__":
    cli()
    
