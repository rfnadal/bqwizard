import click
from .commands import dataset 

@click.group()
@click.pass_context
def cli(ctx):
    """A Big Query CLI Tool for those in a hurry."""
    ctx.ensure_object(dict)
    pass

cli.add_command(dataset.dataset)

if __name__=="__main__":
    cli()
    
