# build on top of dask
FROM docker-hub.uncharted.software/daskdev/dask:2021.2.0

# create the app directory where the flow script(s) are stored
RUN mkdir wm_data_pipeline
WORKDIR /wm_data_pipeline

# copy over dependencies and install them
COPY src/requirements.txt .
RUN pip install -r requirements.txt

# copy over the flow source
ADD src/flows ./flows

