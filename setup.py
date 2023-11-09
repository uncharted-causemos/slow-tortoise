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
        "prefect==1.4.1",
        "dask==2023.9.2", # Make sure this version is same as the dask base image version in src/infra/docker/Dockerfile
        "dask[dataframe]==2023.9.2",
        "dask[distributed]==2023.9.2",
        "pandas==2.1.1",
        "fastparquet==2023.8.0",
        "bokeh==3.2.2",
        "boto3==1.21.21",
        "protobuf==4.21.4",
        "s3fs==2022.5.0"
    ],
    extras_require={
        "dev": [
            "python-dotenv==1.0.0",
            "black==23.9.1",
            "mypy==1.5.1",
            "pytest==7.4.2",
            "pytest-watch==4.2.0",
            "moto[ec2,s3,all]==4.2.7", # for mocking s3
            "elasticsearch==7.17.7"
        ]
    }
)
