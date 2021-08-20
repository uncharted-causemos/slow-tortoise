from dask.distributed import Client
import pandas as pd
import dask.dataframe as dd

from flows.tile_v0 import get_feature_to_regions

if __name__ == '__main__':
    client = Client()
    data = {
        'feature': ["f1", "f1"],
        'country': ["Canada", "Canada"],
        'admin1': ["Ontario", "Ontario"],
        'admin2': ["Simcoe", "Halton"],
        'admin3': ["Barrie", "Burlington"]
    }
    pandas_df = pd.DataFrame(data=data)
    df = dd.from_pandas(pandas_df, npartitions=1)
    result = get_feature_to_regions(df)
    expected = {
        'f1': {'country': ['Canada'],
               'admin1': ['Canada__Ontario'],
               'admin2': ['Canada__Ontario__Halton', 'Canada__Ontario__Simcoe'],
               'admin3': ['Canada__Ontario__Simcoe__Barrie', 'Canada__Ontario__Halton__Burlington']
        }
    }
    df.compute()
    for feature in result:
        for admin_level in result[feature]:
            assert(sorted(result[feature][admin_level]) == sorted(expected[feature][admin_level]))