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
# Show detailed information about a dataset
bqwizard dataset describe my_dataset

# Configuration commands
bqwizard config --help
# Set the GCP project for the current session
bqwizard config set-project my-project-id
# Display current configuration settings
bqwizard config show
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

## Author

- Rolando Franqui (rolandofranqui@gmail.com)
