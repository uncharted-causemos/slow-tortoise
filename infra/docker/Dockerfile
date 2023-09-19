# build on top of dask
FROM docker-hub.uncharted.software/daskdev/dask:2023.9.2-py3.11

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

# copy over the flow source
ADD src/flows ./flows
