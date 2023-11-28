from scripts.gadm.scripts.download_gadm_files import download_gadm
from scripts.gadm.scripts.populate_with_region_names import populate_es_with_gadm

download_gadm()
populate_es_with_gadm()
