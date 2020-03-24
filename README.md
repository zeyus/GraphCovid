# GraphCovid
project to transform the raw case data into something usable.

Using data from https://github.com/CSSEGISandData/COVID-19 (forked: https://github.com/zeyus/COVID-19)

So far, this will import all the CSV files into a sqlite database located in `covidapp/data/covid.db`.
The database contains 2 tables
- `covid_cases` the raw data imported, with deltas
- `cases_by_country` the data aggregated by country per day

## Requirements
- Python 3.5+
- git

## Installation
- Clone the repo
- Do a `git submodule update`

## Running
- `cd covidapp`
- `python main.py`



