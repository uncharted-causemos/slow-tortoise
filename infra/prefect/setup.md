## Prefect Setup

### Dedicated Openstack VM (preferred method)
- Create a new VM on openstack on the same network as the dask server. SSH into VM.
- Download miniconda `curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --output conda.sh`
- Verify hash using `sha256sum conda.sh` and comparing with [the hash here](https://docs.conda.io/en/latest/miniconda.html#linux-installers)
- Install conda with `bash conda.sh`
- Install docker by following [the instructions here](https://docs.docker.com/engine/install/centos/)
- Optional: If you cannot run docker without sudo, run `sudo groupadd docker` and `sudo usermod -aG docker ${USER}`  
  Log out and try again.
- Install docker-compose using [the instructions here](https://docs.docker.com/compose/install/)
- Install prefect and other python requirements. `conda install -c conda-forge python==3.8.6 python-blosc cytoolz prefect dask lz4 nomkl numpy pandas tini==0.18.0`  
  **NOTE:** This line should match whatever was used to build the dask image [here](../dask/base/Dockerfile)
- Copy [config.toml](./config.toml) to `~/.prefect/config.toml` and replace the URL with the external IP of the VM. 
- Configure prefect `prefect backend server`
- **Start prefect server** with `prefect server start`
- In [run_dask_agent.sh](./run_dask_agent.sh) replace the address of the dask cluster and the prefect server using the external IP addresses.  
  Use this script to **start an agent**.

#### NOTE
It is helpful to run the prefect server and agent in their own tmux session.
- Start a new tmux session `tmux new -s <label>`
- Attach to a session `tmux a -t <label>`
- Detach a session `ctrl-b + d`

---

### Using a custom docker swarm
This method is a lot more fiddly. You are essentially redoing the functionality of `prefect server start`.  
See the instructions [here](https://gitlab.uncharted.software/dchang/dask-cluster-example/-/blob/master/prefect-swarm-example/README.md) or [here](https://github.com/flavienbwk/prefect-docker-compose)