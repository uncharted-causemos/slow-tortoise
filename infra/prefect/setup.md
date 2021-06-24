## Prefect Setup

### Dedicated Openstack VM (preferred method)
**Setup**
- Create a new VM on openstack on the same network as the dask server. SSH into VM.
- Download miniconda `curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --output conda.sh`
- Verify hash using `sha256sum conda.sh` and comparing with [the hash here](https://docs.conda.io/en/latest/miniconda.html#linux-installers)
- Install conda with `bash conda.sh`
- Install docker by following [the instructions here](https://docs.docker.com/engine/install/centos/)
- Optional: If you cannot run docker without sudo, run `sudo groupadd docker` and `sudo usermod -aG docker ${USER}`
  Log out and try again.
- Install docker-compose using [the instructions here](https://docs.docker.com/compose/install/)
- A virtual conda environment will be used to simplify future package changes.
  Create a conda environment and install prefect:
  `conda create -n prefect -c conda-forge "prefect==0.14.20"`
- Activate the new conda environment with `conda activate prefect`
- Copy [config.toml](./config.toml) to `~/.prefect/config.toml` making sure to replace the apollo_url with the external IP of the VM and the dask address with the IP of the dask cluster.
- Configure prefect `prefect backend server`
- Create a `prefect` directory and copy [start_server.sh](./start_server.sh) and [start_agent.sh](./start_agent.sh) into it.

**Running the server**
- Make sure the conda evironment is active by running `conda activate prefect`
- Start the prefect server by running `prefect/start_server.sh`
- Start an agent with `prefect/start_agent.sh`.  In the script, the address of the API server should be set to the external IP of the VM, and the `WM_DASK_SCHEDULER` address should be set to point to a running Dask cluster.

**NOTES**
If you are running into issues with docker registry pull limits when starting the server you can run using a specific image version that you already have locally.
- Use `docker images` to find out what versions of prefect you already have in your local registry
- Start the server with something like this `prefect server start --use-volume --skip-pull --version=core-0.14.6 --ui-version=core-0.14.6`

It is helpful to run the prefect server and agent in their own tmux session.
- Start a new tmux session `tmux new -s <label>`
- Attach to a session `tmux a -t <label>`
- Detach a session `ctrl-b + d`

---

### Using a custom docker swarm
This method is a lot more fiddly. You are essentially redoing the functionality of `prefect server start`.
See the instructions [here](https://gitlab.uncharted.software/dchang/dask-cluster-example/-/blob/master/prefect-swarm-example/README.md) or [here](https://github.com/flavienbwk/prefect-docker-compose)


## Prefect Upkeep

Flows can be registered from the developer's machine, and **do not** require logging into the Prefect VM.  Whenever dependencies change in the `setup.py` file, or flow source is modified, the base docker image must be rebuilt, and the updated flows re-registered.
- Make sure the `prefect` conda env is activated `conda activate prefect`
- Re-build the base docker image by running [infra/docker/docker_build.sh](../docker/docker_build.sh) followed by [infra/docker/docker_push.sh](../docker/docker_push.sh)
- Register flows by running [prefect/register_flows.sh](../prefect/register_flows.sh).
- **NOTE:** The updated base image also needs to be deployed to the Dask swarm so that any new dependencies or updates local modules are available.  See instructions [here](../dask/setup.md).
