## Prefect Setup

### Dedicated Openstack VM (preferred method)
- See [here](https://gitlab.uncharted.software/WM/wm-playbooks/-/blob/main/prefect-v1/server/README.md) how to set up prefect server.
- See [here](https://gitlab.uncharted.software/WM/wm-playbooks/-/blob/main/prefect-v1/agents/README.md) how to set up prefect agents.

If prefect server and the agents are running in same machine, It is helpful to run the prefect server and agent in their own tmux session.
- Start a new tmux session `tmux new -s <label>`
- Attach to a session `tmux a -t <label>`
- Detach a session `ctrl-b + d`

---

## Prefect Upkeep
- Activate a suitable virtual environment (venv, conda, etc.) where the prefect is installed in. 
- Re-build the base docker image by running [infra/docker/docker_build.sh](../docker/docker_build.sh) followed by [infra/docker/docker_push.sh](../docker/docker_push.sh)
- Register flows by running [prefect/register_flows.sh](../prefect/register_flows.sh).
- **NOTE:** The updated base image also needs to be deployed to the Dask swarm so that any new dependencies or updates local modules are available.  See instructions [here](../dask/setup.md).
