# Development

From a suitable virtual  environment (venv, conda, etc.), dependencies for local development can be installed by running
```
pip install -e .
```
or
```
conda develop .
```
(see this [issue](https://github.com/conda/conda-build/issues/1992) for uncertainty surrounding the conda command).

For development, flows can be run locally with no requirement for access to a Prefect server or agent, and no Dask cluster instance (an in-process temporary cluster is started by the prefect Dask executor).  The [run_local.sh](./run_local.sh) script provides an example of the environment configuration and execution command to required to run in such a context.  A small amount of boilerplate needs to be added a flow to support this, as can be seen in [dask_flow_test.py](./dask_flow_test.py)

If a cluster is required for performance reasons, but there is no need for full execution in the Prefect infrastructure, the `WM_DASK_SCHEDULER` environment variable can be set to point to an available Dask cluster.  This can be the instance on the docker swarm, or a local docker instance launched by running `docker-compose up` in the `dask` directory.

To validate Prefect execution outside of the deployment environment, a prefect server can be started by running [infra/prefect/start_server.sh](../infra/prefect/start_server.sh), and a docker agent can be started by running [infra/prefect/start_agent_local.sh](../infra/prefect/start_agent_local.sh).  Flows can then be registered as described in the [prefect setup](../infra/prefect/setup.md).  The scripts assume that a Dask cluster will be running locally in this context.

# Deployment

To deploy changes to the production Prefect environment:

1. Re-build the base docker image by running [infra/docker/docker_build.sh](../infra/docker/docker_build.sh)
2. Push the base image [infra/docker/docker_push.sh](../infra/docker/docker_push.sh)
3. Register flows by running [infra/prefect/register_flows.sh](../infra/prefect/register_flows.sh) or executing similar commands for individual flows.
4. Log into the Dask swarm and restart with the updated image (see [here](./infra/dask/setup.md))