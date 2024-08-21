# Data_Pipeline_Optimization
This repo contains a case study for a data pipeline optimization. The input is a parquet file which passes from an optimization process
such as cleaning, fixing types of data and finally stores some information to two SQL Postgres Database.

# Repo Structure:
```
DATA_PIPELINE_OPTIMIZATION/
├── files/                         # Datasets and raw data files
│   └── parquet/                   # Parquet files
│
├── scripts/                       # Code for the project
│   ├── __init__.py                # Makes src a Python module
│   └── utils                      # Utility functions and helpers
│
├── README.md                      # Project documentation
├── requirements.txt               # Packages need to be installed
└── run.py                         # File rto run the data pipeline
```
# How to  Begin

Clone the repo localy in your pc and create a virtual environment:
`conda create -n <envname> python 3.9`

Activate your virtual environment:
`conda activate <envname>`

Install all packages:
`pip install -r requirements.txt`