import os
import pathlib

MAX_GADM_INDEX = 4


def get_csv_filename(index):
    this_filename = str(pathlib.Path(__file__).parent.resolve())
    return this_filename + "/../data/gadm36_{}.csv".format(index)
