import sys
import json
import os
from pathlib import Path
from prefect.utilities.debug import raise_on_exception
from flows.data_pipeline import flow

# Parameters for test datasets
parameter_sets = [
    # 0: For testing tiling data
    dict(
        model_id="geo-test-data",
        run_id="test-run-1",
        data_paths=[f"file://{Path(os.getcwd()).parent}/tests/data/geo-test-data.parquet"],
        selected_output_tasks=[
            "compute_global_timeseries",
            # "compute_regional_stats",
            # "compute_regional_timeseries",
            # "compute_regional_aggregation",
            # "compute_tiles",
        ],
    ),
    # 1: Maxhop
    dict(
        model_id="maxhop-v0.2",
        run_id="4675d89d-904c-466f-a588-354c047ecf72",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results/4675d89d-904c-466f-a588-354c047ecf72/4675d89d-904c-466f-a588-354c047ecf72_maxhop-v0.2.parquet.gzip"
        ],
    ),
    # 2: Test Qualifiers
    dict(
        is_indicator=True,
        qualifier_map={
            "fatalities": [
                "event_type",
                "sub_event_type",
                "source_scale",
                "country_non_primary",
                "admin1_non_primary",
            ]
        },
        model_id="_qualifier-test",
        run_id="indicator",
        data_paths=["s3://test/_qualifier-test.bin"],
    ),
    # 3: Test combining multiple parquet files with different columns
    {
        "data_paths": [
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9d7db850-0abe-486f-8979-b1e9ad2ef6ad/9d7db850-0abe-486f-8979-b1e9ad2ef6ad_7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f.1.parquet.gzip",
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9d7db850-0abe-486f-8979-b1e9ad2ef6ad/9d7db850-0abe-486f-8979-b1e9ad2ef6ad_7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f.2.parquet.gzip",
        ],
        "is_indicator": False,
        "model_id": "7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f",
        "qualifier_map": {
            "yield_loss_risk": ["longitude", "latitude", "time"],
            "harvested_area_at_risk": ["NameCrop", "NameIrrigation", "NameCategory"],
        },
        "qualifier_thresholds": {
            "max_count": 10000,
            "regional_timeseries_count": 100,
            "regional_timeseries_max_level": 1,
        },
        "run_id": "9d7db850-0abe-486f-8979-b1e9ad2ef6ad",
    },
    # 4: Test combining multiple parquet files with different columns
    {
        "data_paths": [
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.3.parquet.gzip",
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.2.parquet.gzip",
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.1.parquet.gzip",
        ],
        "is_indicator": False,
        "model_id": "84fd427f-3a7d-473f-aa25-0c0a150ca216",
        "qualifier_map": {
            "export [kcal]": [],
            "import [kcal]": [],
            "supply [kcal]": [],
            "Production [mt]": ["Year"],
            "Consumption [mt]": ["Year"],
            "Ending_stock [mt]": ["Year"],
            "production [kcal]": [],
            "World market_price [US$/mt]": ["Year"],
            "export per capita [kcal pc]": [],
            "import per capita [kcal pc]": [],
            "supply per capita [kcal pc]": [],
            "production change per capita [kcal pc]": [],
        },
        "qualifier_thresholds": {
            "max_count": 10000,
            "regional_timeseries_count": 100,
            "regional_timeseries_max_level": 1,
        },
        "run_id": "f2818712-09f7-49c6-b920-ea21c764d1c7",
    },
    # 5: Invalid timestamps
    dict(  # Invalid timestamps
        model_id="087c3e5a-cd3d-4ebc-bc5e-e13a4654005c",
        run_id="9e1100d5-06e8-48b6-baea-56b3b820f82d",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9e1100d5-06e8-48b6-baea-56b3b820f82d/9e1100d5-06e8-48b6-baea-56b3b820f82d_087c3e5a-cd3d-4ebc-bc5e-e13a4654005c.1.parquet.gzip"
        ],
    ),
    # 6: Testing weight column small
    dict(
        qualifier_map={"sam_rate": ["qual_1"], "gam_rate": ["qual_1"]},
        weight_column="weights",
        model_id="_weight-test-small",
        run_id="test-run-1",
        data_paths=["s3://test/weight-col.bin"],
    ),
    # 7: Testing weight column 2
    dict(  # Weights
        is_indicator=True,
        qualifier_map={
            "Surveyed Area": ["Locust Presence", "Control Pesticide Name"],
            "Control Area Treated": ["Control Pesticide Name"],
            "Estimated Control Kill (Mortality Rate)": ["Control Pesticide Name"],
        },
        weight_column="Locust Breeding",
        model_id="_weight-test-1",
        run_id="indicator",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/39f7959d-a63e-4db4-a54d-24c66184cf82/39f7959d-a63e-4db4-a54d-24c66184cf82.parquet.gzip"
        ],
    ),
    # 8: Testing weight column Big
    dict(  # Real weights
        selected_output_tasks=[
            "compute_global_timeseries",
            "compute_regional_stats",
            "compute_regional_timeseries",
            "compute_regional_aggregation",
        ],
        is_indicator=False,
        qualifier_map={
            "HWAM_AVE": ["year", "mgn", "season"],
            "production": ["year", "mgn", "season"],
            "crop_failure_area": ["year", "mgn", "season"],
            "TOTAL_NITROGEN_APPLIED": ["year", "mgn", "season"],
        },
        weight_column="HAREA_TOT",
        model_id="2af38a88-aa34-4f4a-94f6-a3e1e6630833",
        run_id="test-run-1",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/eba6ca6b-8c7f-44d1-b008-4349491cabf5/eba6ca6b-8c7f-44d1-b008-4349491cabf5_2af38a88-aa34-4f4a-94f6-a3e1e6630833.1.parquet.gzip"
        ],
    ),
    # 9: DC Test 30K records
    dict(
        is_indicator=True,
        model_id="3a013cd3-6064-4888-9cc6-0e9d637c690e",
        run_id="indicator",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/3a013cd3-6064-4888-9cc6-0e9d637c690e/3a013cd3-6064-4888-9cc6-0e9d637c690e.parquet.gzip",
            "https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/3a013cd3-6064-4888-9cc6-0e9d637c690e/3a013cd3-6064-4888-9cc6-0e9d637c690e_1.parquet.gzip",
            "https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/3a013cd3-6064-4888-9cc6-0e9d637c690e/3a013cd3-6064-4888-9cc6-0e9d637c690e_2.parquet.gzip",
        ],
        fill_timestamp=0,
        qualifier_map={
            "data_id": ["event_date"],
            "fatalities": ["event_date", "event_type", "sub_event_type", "actor1"],
        },
    ),
    # 10:
    dict(
        is_indicator=False,
        model_id="2281e058-d521-4180-8216-54832700cedd",
        run_id="22045d57-aa6a-4df6-a11d-793225878dab",
        data_paths=[
            "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/22045d57-aa6a-4df6-a11d-793225878dab/22045d57-aa6a-4df6-a11d-793225878dab_2281e058-d521-4180-8216-54832700cedd.1.parquet.gzip"
        ],
        fill_timestamp=0,
        qualifier_map={
            "max": ["Date", "camp"],
            "min": ["Date", "camp"],
            "data": ["Date", "camp"],
            "mean": ["Date", "camp"],
            "error": ["Date", "camp"],
            "median": ["Date", "camp"],
        },
    ),
    # 11: Related to bug logged in https://gitlab.uncharted.software/WM/slow-tortoise/-/issues/41
    json.loads(
        """
        {
            "data_paths": ["https://jataware-world-modelers.s3.amazonaws.com/transition/datasets/76b6ec52-183e-49db-9b8f-01c0aaf0255c/76b6ec52-183e-49db-9b8f-01c0aaf0255c.parquet.gzip", "https://jataware-world-modelers.s3.amazonaws.com/transition/datasets/76b6ec52-183e-49db-9b8f-01c0aaf0255c/76b6ec52-183e-49db-9b8f-01c0aaf0255c_1.parquet.gzip"],
            "fill_timestamp": 0,
            "is_indicator": true,
            "model_id": "76b6ec52-183e-49db-9b8f-01c0aaf0255c",
            "raw_count_threshold": 10000,
            "run_id": "indicator",
            "selected_output_tasks": null,
            "weight_column": ""
        } 
    """
    ),
    # 12 Dataset with no available region columns (more details: https://gitlab.uncharted.software/WM/slow-tortoise/-/issues/45#note_404940)
    json.loads(
        """
        {
            "data_paths": ["https://jataware-world-modelers.s3.amazonaws.com/transition/datasets/75a8ad46-a535-4557-9137-0033d8bd2531/75a8ad46-a535-4557-9137-0033d8bd2531.parquet.gzip"],
            "fill_timestamp": 0,
            "is_indicator": true,
            "model_id": "test_indicator",
            "raw_count_threshold": 10000,
            "run_id": "indicator",
            "selected_output_tasks": null,
            "weight_column": ""
        } 
    """
    ),
]

if __name__ == "__main__":
    with raise_on_exception():
        parameters_set_index = 0 if len(sys.argv) < 2 else int(sys.argv[1])
        flow.run(parameters=parameter_sets[parameters_set_index])
