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
  Create a conda environment and install the python dependencies.
  `conda create -n prefect -c conda-forge "python>=3.8.0" prefect dask lz4 fastparquet python-snappy s3fs boto3 "protobuf>=3.13.0"`  
  **NOTE:** The packages should match whatever was used to build the dask image [here](../dask/base/Dockerfile)
- Activate the new conda environment with `conda activate prefect`
- Copy [config.toml](./config.toml) to `~/.prefect/config.toml` making sure to replace the apollo_url with the external IP of the VM and the dask address with the IP of the dask cluster. 
- Configure prefect `prefect backend server`

**Running the server**
- Make sure the conda evironment is active by running `conda activate prefect`
- Start the prefect server with `prefect server start --use-volume`
- Start an agent with `prefect agent local start --api 'http://10.65.18.52:4200/'` where the address is the external IP of the VM.
  
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
