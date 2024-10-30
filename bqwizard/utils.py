from google.cloud import bigquery


def get_client():
    return bigquery.Client()