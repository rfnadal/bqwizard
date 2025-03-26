# BQWizard

A powerful and efficient CLI tool for interacting with Google BigQuery, designed for those who need quick and easy access to their BigQuery resources.

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
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

## Author

- Rolando Franqui (rolandofranqui@gmail.com)
