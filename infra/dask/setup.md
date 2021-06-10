## Dask Setup

### Openstack
- Create a docker image by running `docker_build.sh` from the `infra/docker` directory, and push the image to the registry by running `docker_push.sh`.
- Use [wm-playbooks/sif](https://gitlab.uncharted.software/WM/wm-playbooks/-/tree/master/sif) to create a set of VMs and configure them as a swarm.
- SSH into one of the VMs and copy over or checkout this directory.
- **Deploy** to docker swarm by running `docker stack deploy --compose-file docker-compose.yml dask_swarm`
- Docker deploy will take ~15 seconds. Verify that the services are running with `docker ps` and check the logs with `docker service logs dask_swarm_worker` and `docker service logs dask_swarm_scheduler`
- Test the dask cluster by running [dask_test.py](./dask_test.py) from your local machine.
- To **stop** the docker services run `docker stack rm dask_swarm`

## Updating the image
- Stop the swarm with `docker stack rm dask_swarm`
- If not yet done, update the docker image by running [docker_build.sh](../docker/docker_build.sh) from the `infra/docker` directory, and push the image to the registry by running [docker_push.sh](../docker/docker_push.sh).