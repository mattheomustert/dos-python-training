import click
import pandas as pd


@click.command()
@click.argument("raw_data_file_path", type=click.STRING)
@click.argument("parquet_format_file_path", type=click.STRING)
def convert_csv_to_parquet(raw_data_file_path: str, parquet_format_file_path: str):
    # TODO: add Path objects
    # TODO: add datatype validation
    # TODO: add docstring
    # TODO: add success message
    df = pd.read_csv(raw_data_file_path, sep=';')

    return df.to_parquet(parquet_format_file_path)


if __name__ == '__main__':
    convert_csv_to_parquet()
