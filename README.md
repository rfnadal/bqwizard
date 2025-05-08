# BQWizard

The duct tape and WD-40 every data engineer needs in their Big Query toolbox.

## Features

- Fast and intuitive command-line interface
- Dataset management capabilities
- Built with modern Python practices
- Easy to extend with new commands

## Requirements

- Python 3.11 or higher
- Google Cloud credentials configured
- Google Cloud BigQuery API enabled

## Development/Install

To set up the development environment:

1. Clone the repository:
```bash
git clone https://github.com/rfnadal/bqwizard.git
cd bqwizard
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e .
```

## Usage

BQWizard provides a simple and intuitive interface to interact with BigQuery. Here are some basic commands:

```bash
# Show help
bqwizard --help

# Dataset commands
bqwizard dataset --help
# List all datasets in the current project
bqwizard dataset ls
# List all tables in a dataset
bqwizard dataset tables my_dataset
# Create a chain of datasets with views
bqwizard dataset chain dataset1 dataset2 dataset3
# Create a chain and auto-create missing datasets
bqwizard dataset chain dataset1 dataset2 dataset3 --force
# Create a chain including only tables from a CSV file
bqwizard dataset chain dataset1 dataset2 dataset3 --tables-csv tables.csv
# Show detailed information about a dataset
bqwizard dataset describe my_dataset

# Table commands
bqwizard table --help
# Show detailed information about a table
bqwizard table describe my_dataset.my_table
# Delete a table (requires confirmation)
bqwizard table delete my_dataset.my_table
# Refresh a view to update its schema
bqwizard table refresh_view my_dataset.my_view
# Display the first few rows of a table
bqwizard table head my_dataset.my_table
# Display more rows (default is 5)
bqwizard table head my_dataset.my_table --rows 10
# Export a sample of the table to CSV
bqwizard table sample my_dataset.my_table --percent 5 --filename sample.csv

# Configuration commands
bqwizard config --help
# Set the GCP project for the current session
bqwizard config set-project my-project-id
# Display current configuration settings
bqwizard config show
```

## CSV Table Filtering for Dataset Chain

When creating a dataset chain, you can selectively include only specific tables by providing a CSV file:

1. Create a single-column CSV file containing the table names to include:
```
table1
table2
table3
```

2. Use the `--tables-csv` option with the chain command:
```bash
bqwizard dataset chain dataset1 dataset2 dataset3 --tables-csv path/to/tables.csv
```

Only tables listed in the CSV file will be included in the chain, and others will be skipped.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

## Author

- Rolando Franqui (rolandofranqui@gmail.com)
