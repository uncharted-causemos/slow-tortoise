## Prefect + Dask Infrastructure

### Python environment

- Install requirements into your env using `pip install -r requirements.txt`
- Alternatively, if using `conda`, create a new environment with all the requirements using:   
`conda create -n data-pipeline jupyterlab python=3.8 dask pyarrow s3fs graphviz`
Activate the environment with `conda activate data-pipeline`. Deactivate with `conda deactivate`

---

### Running Jupyter Lab
   
Start `jupyter lab`

---

### Infrastructure

See [here](./infra/README.md) how to set up a dask cluster and prefect server.