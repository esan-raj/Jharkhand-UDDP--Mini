from datetime import datetime

from uddp.pipeline import scheduled_batch_id, window_batch_id


def test_airflow_retries_reuse_the_logical_date():
    logical = datetime(2026, 11, 6, 0, 0, 0)
    assert scheduled_batch_id(logical) == "airflow-20261106"
    assert scheduled_batch_id(logical) == scheduled_batch_id(datetime(2026, 11, 6, 18, 0, 0))


def test_cli_window_reuses_a_batch_id_inside_the_same_window():
    assert window_batch_id(10, now=15) == window_batch_id(10, now=19)
    assert window_batch_id(10, now=15) != window_batch_id(10, now=25)
