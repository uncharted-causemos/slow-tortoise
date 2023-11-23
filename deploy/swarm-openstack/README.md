## Deployment

To deploy the datacube pipeline to the Openstack Prefect environment run [./build_and_update.sh](./build_and_update.sh)

```
./build_and_update.sh
```

**NOTES:**

- **This script will kill the Dask swarms and any running jobs. Before running, you should verify that there are no jobs running.**
- This script assumes:
  - You have SSH access to both dask swarms, with some required entries in your `~/.ssh/config`.
  - The Prefect tooling was configured with `prefect backed server`

The script does the following steps:

1. Stop the request-queue
1. Stop both Dask swarms
1. Re-build the base docker image by running [infra/docker/docker_build.sh](../../infra/docker/docker_build.sh)
1. Push the base image [infra/docker/docker_push.sh](../../infra/docker/docker_push.sh)
1. Log into the Dask swarm and restart with the updated image (see [here](../../infra/dask/setup.md))
1. Register flows by running [infra/prefect/register_flows.sh](../../infra/prefect/register_flows.sh) or executing similar commands for individual flows.
1. Start the request-queue

---
