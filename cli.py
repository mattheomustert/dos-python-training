import click
import pandas as pd
from campus_recruitment_etl.dependency_injection.container import ServiceContainer

service_container = ServiceContainer()


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
def upload_file(local_file_path: str, gcs_file_path: str):
    gcs_client = service_container.gcs_storage_client()

    gcs_client.upload_to_gcs(local_file_path, gcs_file_path)


@cli.command("filter")
@click.argument("filter_", type=click.STRING)
def show_df(filter_):
    gcs_client = service_container.gcs_storage_client()

    download = gcs_client.list_files(filter_)

    print(download)


@cli.command("update_dim")
def update_dim():
    storage_client = service_container.gcs_storage_client()
    path = service_container.config.gcs.parquet_path()
    files = storage_client.list_files(path)

    transformer = service_container.dimension_student_transformer(src_file_paths=files)
    transformer.transform()

    transformer = service_container.dimension_opleiding_transformer(src_file_paths=files)
    transformer.transform()


if __name__ == '__main__':
    cli()
