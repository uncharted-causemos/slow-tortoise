FROM docker-hub.uncharted.software/continuumio/miniconda3:4.8.2

RUN conda install --yes \
    -c conda-forge \
    python==3.8.6 \
    python-blosc \
    cytoolz \
    prefect \
    dask \
    lz4 \
    nomkl \
    numpy \
    pandas \
    tini==0.18.0 \
    && conda clean -tipsy \
    && find /opt/conda/ -type f,l -name '*.a' -delete \
    && find /opt/conda/ -type f,l -name '*.pyc' -delete \
    && find /opt/conda/ -type f,l -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -type f,l -name '*.js' -not -name '*.min.js' -delete \
    && rm -rf /opt/conda/pkgs

COPY prepare.sh /usr/bin/prepare.sh

RUN mkdir /opt/app

ENTRYPOINT ["tini", "-g", "--", "/usr/bin/prepare.sh"]
