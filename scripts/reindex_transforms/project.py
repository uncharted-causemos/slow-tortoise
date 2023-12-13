def transform_fn(doc):
    # Modify document and return
    doc["name"] = "new " + doc["name"]
    return doc
