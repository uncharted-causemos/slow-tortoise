# build on top of dask
FROM docker-hub.uncharted.software/daskdev/dask:2023.9.2-py3.11

# Remove pyarrow optional dependency that comse with the base image to make sure dask to not use pyarrow engine by default
# pyarrow data types with dask seems not matured enough and has performance issues in some cases. 
RUN pip uninstall -y pyarrow

# Install build tools required to build some of python packages 
RUN apt-get update -y && apt-get install gcc g++ -y

# the prefect docker flow registration step always upgrades pip, so if we do the same here we can
# ensure that flow registration uses this cached layer
RUN pip install --upgrade pip

# create the app directory where the flow script(s) are stored
RUN mkdir wm_data_pipeline
WORKDIR /wm_data_pipeline

# copy over dependencies and install them
COPY src/setup.py .
COPY src/version.py .

RUN pip install -e .

# Set prefcet backend configuration to server
RUN prefect backend server

# copy over the flow source
ADD src/flows ./flows
