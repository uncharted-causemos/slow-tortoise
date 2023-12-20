# Data migration Dec 19, 2023
#
# Migrate data_view for indicator datacube, and the data_state and view_state from the linked insight for model datacube
# to data_state property of the datacube
def get_break_down_state(
    breakdown_option,
    run_ids,
    selected_feature,
    selected_region_ids,
    selected_years,
    active_reference_options,
    selected_output_variables,
    selected_qualifier_values,
    baseline_timeseries_id,
):
    comparisonSettings = {
        "shouldDisplayAbsoluteValues": baseline_timeseries_id == "",
        "baselineTimeseriesId": baseline_timeseries_id,
        "shouldUseRelativePercentage": True,
    }
    # If breakdown option is none
    no_breakdown_state = {
        "outputName": selected_feature,
        "modelRunIds": run_ids,
        "comparisonSettings": comparisonSettings,
    }

    if len(run_ids) == 0:
        return no_breakdown_state

    if breakdown_option == "region":
        return {
            "modelRunId": run_ids[0],
            "outputName": selected_feature,
            "regionIds": selected_region_ids,
            "comparisonSettings": comparisonSettings,
        }
    if breakdown_option == "year":
        return {
            "modelRunId": run_ids[0],
            "outputName": selected_feature,
            "regionId": selected_region_ids[0] if len(selected_region_ids) > 0 else None,
            "years": selected_years,
            "comparisonSettings": comparisonSettings,
            "isAllYearsReferenceTimeseriesShown": "allYears" in active_reference_options,
            "isSelectedYearsReferenceTimeseriesShown": "selectYears" in active_reference_options,
        }
    if breakdown_option == "variable":
        return {
            "modelRunId": run_ids[0],
            "outputNames": selected_output_variables,
            "comparisonSettings": comparisonSettings,
        }

    #  qualifier
    if isinstance(breakdown_option, str) and len(breakdown_option) > 0:
        return {
            "modelRunId": run_ids[0],
            "outputName": selected_feature,
            "regionId": selected_region_ids[0] if len(selected_region_ids) > 0 else None,
            "qualifier": breakdown_option,
            "qualifierValues": selected_qualifier_values,
            "comparisonSettings": comparisonSettings,
        }

    # Else, no breakdown
    return no_breakdown_state


def transform_fn(document, **args):
    is_model = document["type"] == "model"

    admin_levels = ["country", "admin1", "admin2", "admin3"]

    # Set default values
    doc_default_view = document.get("default_view", {})
    breakdown_option = doc_default_view.get("breakdownOption", None)
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
    selected_feature_name = document.get("default_feature", "")

    selected_model_run_ids = ["indicator"]
    selected_transform = ""
    selected_timestamp = None

    # data_state defaults
    selected_region_ids = []
    selected_years = []
    active_reference_options = []
    selected_output_variables = []
    selected_qualifier_values = []
    baseline_timeseries_id = ""

    if is_model:
        selected_model_run_ids = []
        # If model, fetch insight and load the state from it
        docs = args["source_client"].search(
            index="insight",
            size=1,
            _source=["view_state", "data_state"],
            query={
                "bool": {
                    "filter": [
                        {"term": {"visibility": "public"}},
                        {"term": {"context_id": data_id}},
                    ]
                }
            },
        )
        if len(docs["hits"]["hits"]) > 0:
            doc = docs["hits"]["hits"][0]["_source"]
            view_state = doc.get("view_state", {})
            breakdown_option = view_state.get("breakdownOption", breakdown_option)
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

            # data_state
            data_state = doc.get("data_state", {})
            selected_model_run_ids = data_state.get("selectedScenarioIds", selected_model_run_ids)
            selected_transform = data_state.get("selectedTransform", selected_transform)
            selected_timestamp = data_state.get("selectedTimestamp", selected_timestamp)
            selected_region_ids = data_state.get("selectedRegionIds", selected_region_ids)
            baseline_timeseries_id = data_state.get("relativeTo", baseline_timeseries_id)
            selected_years = data_state.get("selectedYears", selected_years)
            active_reference_options = data_state.get(
                "activeReferenceOptions", active_reference_options
            )
            selected_output_variables = data_state.get(
                "selectedOutputVariables", selected_output_variables
            )
            selected_qualifier_values = data_state.get(
                "selectedQualifierValues", selected_qualifier_values
            )

            # Overwrite the state with selected feature state
            selected_feature_obj = data_state["activeFeatures"][selected_output_index]
            selected_feature_name = selected_feature_obj.get("name", selected_feature_name)
            selected_transform = selected_feature_obj.get("transform", selected_transform)
            temporal_resolution = selected_feature_obj.get(
                "temporalResolution", temporal_resolution
            )
            spatial_aggregation_method = selected_feature_obj.get(
                "spatialAggregation", spatial_aggregation_method
            )
            temporal_aggregation_method = selected_feature_obj.get(
                "temporalAggregation", temporal_aggregation_method
            )
    default_state = {
        "dataId": data_id,  # Do we need this?
        "breakdownState": get_break_down_state(
            breakdown_option,
            selected_model_run_ids,
            selected_feature_name,
            selected_region_ids,
            selected_years,
            active_reference_options,
            selected_output_variables,
            selected_qualifier_values,
            baseline_timeseries_id,
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
        "selectedTimestamp": selected_timestamp,
        "selectedTransform": selected_transform,
        "spatialAggregationMethod": spatial_aggregation_method,
        "temporalAggregationMethod": temporal_aggregation_method,
        "spatialAggregation": spatial_aggregation,
        "temporalResolution": temporal_resolution,
    }
    document["default_state"] = default_state
    return document
