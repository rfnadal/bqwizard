# BQWizard

The duct tape and WD-40 every data engineer needs in their Big Query toolbox.

## Features

- **Fast and intuitive command-line interface** for BigQuery operations
- **Layer management system** for multi-project/multi-environment workflows
- **Advanced dataset chaining** with dry-run and selective table filtering
- **Comprehensive table operations** including sampling, view management, and data exposure
- **Cross-project data workflows** with simplified layer-based references
- **Enhanced configuration management** for projects and environments
- **Built with modern Python practices** and easy to extend with new commands

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

## Quick Start

```bash
# Show help
bqwizard --help

# Set up your project
bqwizard config set-project my-project-id

# List datasets and tables
bqwizard dataset ls
bqwizard dataset tables my_dataset

# Create a simple dataset chain
bqwizard dataset chain dataset1 dataset2 dataset3
```

## Layer Management System

BQWizard's layer system simplifies working with multi-project data pipelines by allowing you to define layer-to-project mappings.

### Setting up Layers

```bash
# Configure layers for different environments
bqwizard config set-layer raw my-project-raw
bqwizard config set-layer staging my-project-staging
bqwizard config set-layer analytics my-project-analytics

# List all configured layers
bqwizard config list-layers

# Get project for specific layer
bqwizard config get-layer raw

# Remove a layer
bqwizard config remove-layer staging
```

### Using Layers

Once configured, you can use layer references instead of full project names:

```bash
# Instead of: my-project-raw.customer_data
# Use: raw.customer_data

# Create cross-project chains using layers
bqwizard dataset chain raw.customer_data staging.customer_data analytics.customer_data

# Use layer shorthand for multiple datasets
bqwizard dataset chain --layers raw,staging,analytics --datasets customer_data,user_data
```

## Dataset Commands

### Basic Dataset Operations

```bash
# List all datasets in the current project
bqwizard dataset ls

# List all tables in a specific dataset
bqwizard dataset tables my_dataset
bqwizard dataset tables project.dataset  # Cross-project

# Show detailed information about a dataset
bqwizard dataset describe my_dataset
bqwizard dataset describe raw.customer_data  # Using layer reference

# Show detailed information about all datasets
bqwizard dataset describe-all

# Create a new dataset
bqwizard dataset create new_dataset --location US

# Delete a dataset (requires double confirmation)
bqwizard dataset delete old_dataset
```

### Advanced Dataset Chaining

```bash
# Basic dataset chain (creates views in dataset2 from dataset1, views in dataset3 from dataset2)
bqwizard dataset chain dataset1 dataset2 dataset3

# Chain with automatic dataset creation
bqwizard dataset chain dataset1 dataset2 dataset3 --force

# Chain only specific tables
bqwizard dataset chain dataset1 dataset2 dataset3 --tables "table1,table2,table3"

# Preview changes without executing (dry-run)
bqwizard dataset chain dataset1 dataset2 dataset3 --dry-run

# Cross-project chaining using layers
bqwizard dataset chain raw.customer_data staging.customer_data analytics.customer_data

# Layer shorthand for multiple datasets
bqwizard dataset chain --layers raw,staging,analytics --datasets customer_data,user_data

# Combined options
bqwizard dataset chain --layers raw,staging --datasets customer_data --tables "orders,payments" --dry-run
```

### Cross-Project Dataset Exposure

```bash
# Create views in target dataset from source dataset tables
bqwizard dataset expose source_project source_dataset target_project target_dataset

# With automatic target dataset creation
bqwizard dataset expose source_project source_dataset target_project target_dataset --force
```

## Table Commands

### Basic Table Operations

```bash
# Show detailed information about a table
bqwizard table describe my_dataset.my_table
bqwizard table describe raw.customer_data.orders  # Using layer reference

# Delete a table (requires double confirmation)
bqwizard table delete my_dataset.my_table

# Display the first few rows of a table
bqwizard table head my_dataset.my_table
bqwizard table head my_dataset.my_table --rows 10  # More rows

# Refresh a view to update its schema
bqwizard table refresh_view my_dataset.my_view

# Export a sample of the table to CSV
bqwizard table sample my_dataset.my_table --percent 5 --filename sample.csv

# Create a view of a table in another dataset
bqwizard table expose source_dataset.source_table target_dataset.target_table

# Create view with automatic target dataset creation
bqwizard table expose source_dataset.source_table target_dataset.target_table --force
```

### Advanced Table Chaining

```bash
# Create specific tables as views across multiple datasets
bqwizard table chain dataset1.table1 dataset1.table2 dataset2 dataset3

# With dry-run preview
bqwizard table chain dataset1.table1 dataset1.table2 dataset2 dataset3 --dry-run

# With automatic dataset creation
bqwizard table chain dataset1.table1 dataset1.table2 dataset2 dataset3 --force

# Cross-project table chaining
bqwizard table chain raw.customer_data.orders staging.customer_data analytics.customer_data
```

### Table Analysis

```bash
# View table genealogy/dependencies
bqwizard table genealogy my_dataset.my_view
```

## Configuration Commands

```bash
# Set the active GCP project
bqwizard config set-project my-project-id

# Display current configuration settings
bqwizard config show

# Layer management
bqwizard config set-layer raw my-project-raw
bqwizard config list-layers
bqwizard config get-layer raw
bqwizard config remove-layer raw
```

## Common Workflows

### Multi-Environment Data Pipeline

Setting up a complete data pipeline across environments:

```bash
# 1. Configure layers for your environments
bqwizard config set-layer raw my-company-raw
bqwizard config set-layer staging my-company-staging
bqwizard config set-layer analytics my-company-analytics

# 2. Create pipeline for customer data
bqwizard dataset chain --layers raw,staging,analytics --datasets customer_data --dry-run

# 3. Execute after reviewing dry-run results
bqwizard dataset chain --layers raw,staging,analytics --datasets customer_data

# 4. Add more datasets to the same pipeline
bqwizard dataset chain --layers raw,staging,analytics --datasets user_data,product_data
```

### Cross-Project Data Sharing

Expose data from one project to another:

```bash
# Method 1: Expose entire dataset
bqwizard dataset expose source_project raw_data target_project shared_data --force

# Method 2: Expose specific tables
bqwizard table expose source_project.raw_data.customers target_project.shared_data.customers --force
bqwizard table expose source_project.raw_data.orders target_project.shared_data.orders --force
```

### Selective Data Processing

Process only specific tables through your pipeline:

```bash
# Only process critical tables
bqwizard dataset chain --layers raw,staging,analytics --datasets customer_data --tables "orders,payments,subscriptions"

# Chain specific tables across datasets
bqwizard table chain raw.customer_data.orders raw.customer_data.payments staging.customer_data analytics.customer_data
```

## Best Practices

### Layer Naming Conventions

- Use descriptive, consistent layer names: `raw`, `staging`, `clean`, `analytics`, `ml`, `reporting`
- Include environment in project names: `mycompany-raw-prod`, `mycompany-staging-dev`
- Keep layer names short for easier CLI usage

### Safe Operations

- **Always use `--dry-run` first** for complex operations
- Use descriptive dataset names that include the data domain
- Verify layer configurations with `bqwizard config list-layers` before bulk operations

### Performance Tips

- Use `--tables` filtering to process only necessary tables
- Consider the dependency order when chaining datasets
- Use layer shorthand for bulk operations across multiple datasets

## Troubleshooting

### Common Issues

**Layer not found error:**
```bash
# Check configured layers
bqwizard config list-layers

# Add missing layer
bqwizard config set-layer raw my-project-raw
```

**Permission denied:**
- Ensure you have BigQuery Data Editor role on target projects
- Verify Google Cloud credentials are properly configured: `gcloud auth list`

**Dataset already exists:**
- Use `--force` flag to create missing datasets automatically
- Or create datasets manually before running chain operations

**Table reference format:**
- Single dataset: `table_name` (uses current project)
- Project qualified: `project.dataset.table_name`
- Layer qualified: `layer.dataset.table_name`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

## Author

- Rolando Franqui (rolandofranqui@gmail.com)
