import click
from .commands import dataset 
from google.cloud import bigquery


@click.group()
@click.pass_context
@click.option("--project", envvar='GOOGLE_CLOUD_PROJECT')
def cli(ctx, project):
    """A Big Query CLI Tool for those in a hurry."""
    ctx.ensure_object(dict)
    client = bigquery.Client(project)
    ctx.obj['PROJECT'] = project
    ctx.obj['CLIENT'] = client
    if project is None:
        click.echo("Please pass in a project with the --project flag or configure the GOOGLE_CLOUD_PROJECT environment variable.")
        exit()

cli.add_command(dataset.dataset)

if __name__=="__main__":
    cli()
    
