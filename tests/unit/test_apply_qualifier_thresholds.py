from ..utils import execute_prefect_task
from flows.data_pipeline import apply_qualifier_thresholds


def test_apply_qualifier_thresholds():
    q_map = {
        "f1": ["qual1", "qual2", "qual3"],
        "f2": ["qual1"],
        "f3": ["qual1"],
        "f_invalid": ["qual1"],
    }
    q_cols = [["qual1"], ["qual2"], ["qual3"]]
    counts = {
        "f1": {"qual1": 10, "qual2": 8, "qual3": 10},
        "f2": {"qual1": 15, "qual2": 10},
        "f3": {"qual1": 5, "qual2": 10},
    }
    threshold = {"max_count": 9}

    new_q_map, new_q_cols = execute_prefect_task(apply_qualifier_thresholds)(
        q_map, q_cols, counts, threshold
    )

    assert {"f1": ["qual2"], "f2": [], "f3": ["qual1"]} == new_q_map
    assert [["qual1"], ["qual2"]] == sorted(new_q_cols, key=lambda x: x[0])

