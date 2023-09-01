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
        "dask[complete]==2023.8.1",
        "pandas==2.0.3",
        "pyarrow==13.0.0",
        "boto3==1.21.21",
        "protobuf==4.21.4",
    ],
    extras_require={
        "dev": [
            "python-dotenv==1.0.0",
            "s3fs==2022.5.0", # a tool to mount aws bucket to local fs
        ]
    }
)
