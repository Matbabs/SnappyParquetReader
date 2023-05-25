# Snappy Parquet Reader

The Snappy Parquet Reader is a Python script that reads a Snappy-compressed Parquet file and displays its contents in an HTML table. It uses PySpark, Pandas, and webbrowser libraries to achieve this functionality.

## Usage

1. Ensure you have Python and the required dependencies installed.
2. Run the script with the following command:

   ```shell
   python snappy_reader.py file_path
   ```

   Replace `file_path` with the path to the Snappy-compressed Parquet file you want to read.

3. The script will generate an HTML file with the results and attempt to open it in your default web browser.

   If the browser fails to open automatically, you can open the output file manually located at `~/.snappy_reader.results.html`.

## Requirements

- Python 3.x
- PySpark
- Pandas

## Example

```shell
python snappy_reader.py /path/to/my_file.snappy.parquet
```

## Output

The script reads the specified Snappy-compressed Parquet file and converts it into an HTML table. The resulting table is styled using CSS and saved as an HTML file. Each column in the Parquet file corresponds to a column in the HTML table.

The HTML table provides the following features:

- Sticky header row (the header row remains visible even when scrolling)
- Hover highlighting on rows and cells
- Ellipsis for long text in cells with overflow

## Limitations

- The script assumes that the provided file path is a valid Snappy-compressed Parquet file.
- The script uses the default output file path `~/.snappy_reader.results.html`.
- Opening the HTML file in the browser requires a functioning web browser on your system.
