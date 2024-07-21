# Cestus

![Cestus Logo](https://www.thefightcity.com/wp-content/uploads/2020/07/fists-of-pugilist-statue.jpg)

Cestus is column store format designed for data storage and retrieval, particularly optimized for analytical workloads.

## Features

- **Efficient Storage**: Optimized for storing large volumes of data with minimal space.
- **High Performance**: Designed for fast read and write operations, supporting complex queries.
-  **Statistics Gathering**: Automatically collects and maintains statistics on data distribution, aiding in query optimization and performance tuning.

## Installation

Install dependencies with:

```bash
poetry install
```
## Usage
To use Cestus, follow these steps:

```python
import csv
from cestus.core import CestusReader, CestusWriter

schema = {
    'time_ref': str(),
    'account': str(),
    'code': str(),
    'country_code': str(),
    'product_type': str(),
    'value': float(),
    'status': str()
}


def compress_csv():
    with open('example.csv', newline='') as csvfile:
        example = csv.reader(csvfile, delimiter=',', quotechar='|')
        CestusWriter('revised.cestus').write([elem for elem in example], schema)
    data = CestusReader('revised.cestus').read()
```
