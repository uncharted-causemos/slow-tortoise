# build on top of dask
FROM docker-hub.uncharted.software/daskdev/dask:2021.6.0
# the prefect docker flow registration step always upgrades pip, so if we do the same here we can
# ensure that flow registration uses this cached layer
RUN pip install --upgrade pip

# create the app directory where the flow script(s) are stored
RUN mkdir wm_data_pipeline
WORKDIR /wm_data_pipeline

# copy over dependencies and install them
COPY src/setup.py .
COPY src/version.py .

# force an install of our package requirements so that they get properly
# cached
RUN python3 setup.py egg_info && pip3 install `grep -v '^\[' *.egg-info/requires.txt`

# copy over the flow source
ADD src/flows ./flows

# install the package so that it is properly integrated with the python environment -
# dependencies should be cached from the previous setup call
RUN pip install -e .
