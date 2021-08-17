from flows.tile_v0 import get_feature_to_regions
import pandas as pd

data = {
    'feature': ["f1", "f1"],
    'country': ["Canada", "Canada"],
    'admin1': ["Ontario", "Ontario"],
    'admin2': ["Simcoe", "Halton"],
    'admin3': ["Barrie", "Burlington"]
}
df = pd.DataFrame(data=data)
result = get_feature_to_regions(df, True)
expected = {
    'f1': {'country': ['Canada'],
           'admin1': ['Canada__Ontario'],
           'admin2': ['Canada__Ontario__Halton', 'Canada__Ontario__Simcoe'],
           'admin3': ['Canada__Ontario__Simcoe__Barrie', 'Canada__Ontario__Halton__Burlington']
    }
}
for feature in result:
    for admin_level in result[feature]:
        assert(sorted(result[feature][admin_level]) == sorted(expected[feature][admin_level]))