import click
from .commands import dataset
from .commands import config
from google.cloud import bigquery
from .commands.utils import get_project_from_config


@click.group()
@click.pass_context
@click.option("--project", envvar="GOOGLE_CLOUD_PROJECT")
def cli(ctx, project):
    """A Big Query CLI Tool for those in a hurry."""
    ctx.ensure_object(dict)
    if not project:
        project = get_project_from_config()
    try:
        client = bigquery.Client(project=project)
        ctx.obj["PROJECT"] = project
        ctx.obj["CLIENT"] = client
    except Exception as e:
        click.echo(f"Error initializing BigQuery client: {str(e)}")
        click.echo(
            "Please ensure you have valid Google Cloud credentials and a valid project ID."
        )
        exit(1)


cli.add_command(dataset.dataset)
cli.add_command(config.config)

if __name__ == "__main__":
    cli()
