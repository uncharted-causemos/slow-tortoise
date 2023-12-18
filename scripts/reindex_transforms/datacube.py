# Data migration Dec 19, 2023
#
# Migrate data_view for indicator datacube, and the data_state and view_state from the linked insight for model datacube
# to data_state property of the datacube


def get_break_down_state(
    breakdown_option, run_ids, selected_feature, selected_region_ids, selected_years
):
    comparisonSettings = {
        "shouldDisplayAbsoluteValues": True,
        "baselineTimeseriesId": "",
        "shouldUseRelativePercentage": False,
    }
    # If breakdown option is none
    state = {
        "outputName": selected_feature,
        "modelRunIds": run_ids,
        "comparisonSettings": comparisonSettings,
    }

    if len(run_ids) == 0:
        return state

    if breakdown_option == "region":
        state = {
            "modelRunId": run_ids[0],
            "outputName": selected_feature,
            "regionIds": selected_region_ids,
            "comparisonSettings": comparisonSettings,
        }
    if breakdown_option == "year":
        state = {
            "modelRunId": run_ids[0],
            "outputName": selected_feature,
            "regionId": selected_region_ids[0] if len(selected_region_ids) > 0 else None,
            "years": selected_years,
            "comparisonSettings": comparisonSettings,
            # "isAllYearsReferenceTimeseriesShown": False,
            # "isSelectedYearsReferenceTimeseriesShown": False,
        }

    return state


def transform_fn(document, source_client):
    is_model = document["type"] == "model"

    admin_levels = ["country", "admin1", "admin2", "admin3"]

    # Set default values
    doc_default_view = document.get("default_view", {})
    color_scale_type = doc_default_view.get("colorScaleType", "linear discrete")
    color_scheme_name = doc_default_view.get("colorSchemeName", "DEFAULT")
    spatial_aggregation = admin_levels[doc_default_view.get("selectedAdminLevel", 0)]
    selected_map_data_layer = doc_default_view.get("selectedMapDataLayer", "admin")
    selected_map_base_layer = doc_default_view.get("selectedMapBaseLayer", "default")
    spatial_aggregation_method = doc_default_view.get("spatialAggregation", "mean")
    color_scheme_reversed = doc_default_view.get("colorSchemeReversed", False)
    temporal_resolution = doc_default_view.get("temporalResolution", "month")
    number_of_color_bins = doc_default_view.get("numberOfColorBins", 5)
    data_layer_transparency = doc_default_view.get("dataLayerTransparency", "1")
    temporal_aggregation_method = doc_default_view.get("temporalAggregation", "mean")

    data_id = document.get("data_id", "")
    selected_feature = document.get("default_feature", "")

    selected_model_run_ids = ["indicator"]
    selected_transform = ""
    selected_timestamp = None

    if is_model:
        # If model, fetch insight and load the state from it
        docs = source_client.search(
            query={
                "size": 1,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"visibility": "public"}},
                            {"term": {"context_id": data_id}},
                        ]
                    }
                },
                "_source": ["view_state", "data_state"],
            }
        )
        if len(docs) > 0:
            doc = docs[0]
            view_state = document.get("view_state", {})
            breakdown_option = view_state.get("breakdownOption", "")
            color_scale_type = view_state.get("colorScaleType", color_scale_type)
            color_scheme_name = view_state.get("colorSchemeName", color_scheme_name)
            spatial_aggregation = admin_levels[
                view_state.get("selectedAdminLevel", doc_default_view.get("selectedAdminLevel", 0))
            ]
            selected_map_data_layer = view_state.get(
                "selectedMapDataLayer", selected_map_data_layer
            )
            selected_map_base_layer = view_state.get(
                "selectedMapBaseLayer", selected_map_base_layer
            )
            spatial_aggregation_method = view_state.get(
                "spatialAggregation", spatial_aggregation_method
            )
            color_scheme_reversed = view_state.get("colorSchemeReversed", color_scheme_reversed)
            temporal_resolution = view_state.get("temporalResolution", temporal_resolution)
            number_of_color_bins = view_state.get("numberOfColorBins", number_of_color_bins)
            data_layer_transparency = view_state.get(
                "dataLayerTransparency", data_layer_transparency
            )
            temporal_aggregation_method = view_state.get(
                "temporalAggregation", temporal_aggregation_method
            )
            selected_output_index = view_state.get("selectedOutputIndex", 0)

            # Apply data_state
            data_state = document.get("data_state", {})
            selected_model_run_ids = data_state.get("selectedScenarioIds", [])
            selected_transform = data_state.get("selectedTransform", selected_transform)
            selected_timestamp = data_state.get("selectedTimestamp", selected_timestamp)
            selected_region_ids = data_state.get("selectedRegionIds", [])
            selected_years = data_state.get("selectedYears", [])

            # Overwrite the state with selected feature state
            selected_feature = data_state["activeFeatures"][selected_output_index]
            selected_feature = selected_feature.get("name", selected_feature)
            selected_transform = selected_feature.get("transform", selected_transform)
            temporal_resolution = selected_feature.get("temporalResolution", temporal_resolution)
            spatial_aggregation_method = selected_feature.get(
                "spatialAggregation", spatial_aggregation_method
            )
            temporal_aggregation_method = selected_feature.get(
                "temporalAggregation", temporal_aggregation_method
            )
            # Apply breakdown state

            print(doc)

    default_state = {
        "dataId": data_id,
        "breakdownState": get_break_down_state(
            breakdown_option,
            selected_model_run_ids,
            selected_feature,
            selected_region_ids,
            selected_years,
        ),
        "mapDisplayOptions": {
            "selectedMapBaseLayer": selected_map_base_layer,
            "selectedMapDataLayer": selected_map_data_layer,
            "dataLayerTransparency": data_layer_transparency,
            "colorSchemeReversed": color_scheme_reversed,
            "colorSchemeName": color_scheme_name,
            "colorScaleType": color_scale_type,
            "numberOfColorBins": number_of_color_bins,
        },
        "selectedTimestamp": selected_timestamp,  # Do we need this?
        "selectedTransform": selected_transform,  # empty string or null?
        "spatialAggregationMethod": spatial_aggregation_method,
        "temporalAggregationMethod": temporal_aggregation_method,
        "spatialAggregation": spatial_aggregation,
        "temporalResolution": temporal_resolution,
    }
    document["default_state"] = default_state
    # Modify document and return
    return document
