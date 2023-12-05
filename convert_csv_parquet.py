import click
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()


@click.group()
def cli():
    pass


@cli.command("convert")
@click.argument("raw_data_file_path", type=click.STRING)
@click.argument("parquet_format_file_path", type=click.STRING)
def convert_csv_to_parquet(raw_data_file_path: str, parquet_format_file_path: str):
    # TODO: add Path objects
    # TODO: add datatype validation
    # TODO: add docstring
    # TODO: add success message
    df = pd.read_csv(raw_data_file_path, sep=';')

    return df.to_parquet(parquet_format_file_path)


@cli.command("upload")
@click.argument("local_file_path", type=click.STRING)
@click.argument("gcs_file_path", type=click.STRING)
def upload_to_gcs(local_file_path: str, gcs_file_path: str):
    client = storage.Client()

    bucket = client.bucket(os.environ.get("GCS_BUCKET"))

    blob = bucket.blob(gcs_file_path)

    blob.upload_from_filename(local_file_path)


if __name__ == '__main__':
    cli()
