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
        "prefect==0.14.20",
        "dask==2021.6.0",
        "lz4==3.1.3",
        "blosc==1.10.4",
        "pandas==1.2.4",
        "s3fs==2021.5.0",
        "boto3==1.17.49",
        "protobuf==3.17.3",
        "pyarrow==4.0.1",
    ]
)
