import argparse
import sys
import webbrowser
import os
from pyspark.sql import SparkSession

APP_NAME = "Snappy Parquet Reader"
OUTPUT_FILE = os.path.expanduser("~/.snappy_reader.results.html")
CSS_STYLE = """
    <style>
        .parquet-table {
            font-family: monospace;
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }
        .parquet-table th,
        .parquet-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        .parquet-table th {
            background-color: #4CAF50;
            color: #fff;
        }
        .parquet-table thead:first-child {
            position: sticky;
            top: 0;
            z-index: 1;
            background-color: #4CAF50;
        }
        .parquet-table td {
            background-color: #fff;
            color: #222;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .parquet-table tr:hover {
            background-color: #f5f5f5;
        }
        .parquet-table tr:hover td {
            background-color: #e6e6e6;
        }
    </style>
    """

def parse_args():
    parser = argparse.ArgumentParser(description='Read Snappy Parquet file and display in HTML table.')
    parser.add_argument('file_path', metavar='file_path', type=str, help='Path to .snappy.parquet file')
    return parser.parse_args()

def read_parquet(file_path):
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    return spark.read.parquet(file_path)

def dataframe_to_pandas(df):
    return df.toPandas()

def write_panda_to_html(pdf):
    html_table = pdf.to_html(index=False, classes="parquet-table", table_id="parquet-table")
    with open(OUTPUT_FILE, "w") as file:
        file.write(CSS_STYLE)
        file.write(html_table)

def open_browser():
    try:
        webbrowser.open(OUTPUT_FILE)
    except:
        print("Failed to open web browser. Please open the output file manually.")


if __name__ == "__main__":
    args = parse_args()
    write_panda_to_html(dataframe_to_pandas(read_parquet(args.file_path)))
    open_browser()
