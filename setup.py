from distutils.core import setup

from setuptools import find_packages
from version import __version__

with open("version.py") as f:
    exec(f.read())

setup(
    name="wm-data-pipeline",
    version=__version__,
    description="World modellers data ingest pipeline",
    packages=find_packages(),
    keywords=["world modellers"],
    license="Apache-2.0",
    install_requires=[
        "prefect==1.2.4",
        "dask==2022.7.1", # This dask version should match with the dask base image tag in infra/docker/Dockerfile. e.g  docker-hub.uncharted.software/daskdev/dask:2022.7.1
        "bokeh==2.4.3",
        "lz4==4.0.2",
        "blosc==1.10.6",
        "pandas==1.3.5", # There's a groupby.apply breaking change in >= 1.4.0 version (https://pandas.pydata.org/pandas-docs/stable/whatsnew/v1.4.0.html#groupby-apply-consistent-transform-detection)
        "s3fs==2022.5.0",
        "boto3==1.21.21",
        "protobuf==4.21.4",
        "pyarrow==8.0.0",
        "prometheus_client==0.14.1",
    ],
)
