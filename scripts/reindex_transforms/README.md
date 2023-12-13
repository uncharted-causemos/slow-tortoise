### Reindex Transforms

This folder stores the transforms for `reindex.py` script for light weight ETL. It is useful for doing data migrations where a small number of fields have changed and we can apply an simple self-sustained transform.

Each transfrom python file specifies a `transform_fn` function that modifies the ES-doc, for example here is one where name field is updated with new value.

```
def transform_fn(doc):
  doc["name"] = "new " + doc["name"]
return doc
```
