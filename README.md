# Event_Base
Project- 3 members

## Data sources

This project is based on publicly available data about the Wrocław
public transportation system.

- [data/rozklad-jazdy](https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data)
- [data/rozklad-jazdy-xml](https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoxml_data)

## Running the project

This project is managed using [poetry](https://python-poetry.org/).

```bash
# Install poetry: https://python-poetry.org/docs/
# curl -sSL https://install.python-poetry.org | python3 -

# Activate the environment
poetry env activate

# Check for requirements
pip list

# Install dependencies
poetry install

# Help information
poetry run cli --help

# Run the application
poetry run app
```

## Documentation

When running the server, automatically generated documentation of the API is available at [http://localhost:8000/docs](http://localhost:8000/docs).
