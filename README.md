# causemos-data-pipline

## SuperMaas to Causemos data pipeline

### Python environment

- Install requirements into your env using `pip install -r requirements.txt`
- Alternatively, if using `conda`, create a new environment with all the requirements using:   
`conda create -n data-pipeline -c conda-forge jupyterlab "python>=3.8.0" dask lz4 pyarrow fastparquet python-snappy s3fs boto3 "protobuf>=3.13.0"`
Activate the environment with `conda activate data-pipeline`. Deactivate with `conda deactivate`

---

### Running Jupyter Lab
   
Start `jupyter lab`

---

### Infrastructure

See [here](./infra/README.md) how to set up a dask cluster and prefect server.