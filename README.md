# Dojo to Causemos data pipeline

## Setup

### Python Version

The pipeline is tested and deployed with **Python 3.11**. While small differences to the patch version should work there are some dependencies with known issues on versions other than 3.11.\*.

### Installation and Enabling The Virtual Environment

- Install dependencies

  ```
  make install
  ```

- Enable the virtual environment
  ```
  source .venv/bin/activate
  ```

---

## Setup Code Formatting

This repo uses [black](https://black.readthedocs.io/en/stable/index.html). Once installed, you can configure it in your IDE with the instructions [here](https://black.readthedocs.io/en/stable/integrations/editors.html).  
In VS Code, install 'Black Formatter' extension and add the following lines into `settings.json`

```
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  }
```

You can apply formatting rules to the current document with `Format Document` (Option + Shift + F) or to all files by running `black .` from the repo root.

---

## Setup Linting

The Gitlab CI pipeline uses `mypy`.
After installing, you will need to download type definitions using `mypy --install-types`.  
In VS Code, you can configure it with the `Python: Select Linter` action.

---

## Development

This will locally run the `flow.run` configurations at the bottom of [data_pipeline.py](./flows/data_pipeline.py). The results are written to minio-dev. See the folder structure [here](./doc/minio-folder-structure.md).

```
cd flows
./run_local.sh
```

For development, flows can be run locally with no requirement for access to a Prefect server or agent, and no Dask cluster instance (an in-process temporary cluster is started by the prefect Dask executor). The [run_local.sh](./flows/run_local.sh) script provides an example of the environment configuration and execution command to required to run in such a context. A small amount of boilerplate needs to be added a flow to support this, as can be seen in [dask_flow_test.py](./flows/dask_flow_test.py)

If a cluster is required for performance reasons, but there is no need for full execution in the Prefect infrastructure, the `WM_DASK_SCHEDULER` environment variable can be set to point to an available Dask cluster. This can be the instance on the docker swarm, or a local docker instance launched by running `docker-compose up` in the `dask` directory.

To validate Prefect execution outside of the deployment environment, a prefect server can be started by running [infra/prefect/start_server.sh](./infra/prefect/start_server.sh), and a docker agent can be started by running [infra/prefect/start_agent_local.sh](./infra/prefect/start_agent_local.sh). Flows can then be registered as described in the [prefect setup](./infra/prefect/setup.md). The scripts assume that a Dask cluster will be running locally in this context.

### Notes on Dask development

- Dask is lazy evaluated. Operations are only evaluated when `.compte()` is called.
- In order to `print()` a dataframe you will need to `compute` it or use `print(df.head())`.
- It's easy to write extremely slow code in Dask, stick to vectorized operations and built-in aggregations.
- When working with larger data consider removing parts of the flow that aren't necessary for your work. For example, remove tiling, or the entire annual part of the pipeline.

---

## Deployment

See [here](./deploy/kubernetes/README.md) how to deploy the data pipeline

---

## Infrastructure

See [here](./infra/README.md) how to set up a dask cluster and prefect server.

---

![](./doc/remaasta-flow.png)
