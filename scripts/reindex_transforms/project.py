def transform_fn(doc, **args):
    # Modify document and return
    doc["name"] = "new " + doc["name"]
    return doc
