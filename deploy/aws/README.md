# AWS Kubernetes Data Pipeline Deployment

To deploy new data pipeline code in the AWS production environment, follow these steps:

## 1. Build and Push Docker Image

- A Docker image with the `latest` tag is automatically created during CI when changes are merged into the `master` branch.
- Alternatively, manually build and push the image using the `./build_and_push.sh` script.

## 2. Update Dask Cluster

- Update the Dask scheduler and workers running in the AWS Kubernetes cluster with the newer Docker image.
- If you use the `latest` Docker tag for the pods, recreate the pods for the Dask cluster:
  ```
  export KUBECONFIG=~/.kube/causemos-prod.config
  kubectl get po -n causemos | grep dask
  kubectl delete po POD -n causemos
  ```
  and verify the image hash before/after
  ```
  kubectl describe po POD  -n causemos | grep sha
  ```

## 3. Register Flow

- To register new flow code, run the `register_flows.sh` script.
- For test flows, run `register_test_flows.sh`.