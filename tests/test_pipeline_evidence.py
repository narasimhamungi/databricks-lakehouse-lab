"""summarise_expectations: reading expectation counts out of a declarative
pipeline's event log (plain Python, no Spark)."""

from __future__ import annotations

import json

from lakehouse.tasks import summarise_expectations


def _dq(passed, failed, dropped=0, name="ohlc_ordered"):
    return {
        "dropped_records": dropped,
        "expectations": [{"name": name, "dataset": "d", "passed_records": passed, "failed_records": failed}],
    }


def _progress(update, flow, status, dq, ts, as_string=True):
    details = {"flow_progress": {"status": status, "data_quality": dq}}
    return {
        "event_type": "flow_progress",
        "update_id": update,
        "flow_name": flow,
        "timestamp": ts,
        "details": json.dumps(details) if as_string else details,
    }


def test_counts_only_the_latest_update_and_its_completed_events():
    events = [
        {"event_type": "create_update", "update_id": "u1", "flow_name": None, "timestamp": "1", "details": "{}"},
        _progress("u1", "yf", "COMPLETED", _dq(10, 5), "2"),
        {"event_type": "create_update", "update_id": "u2", "flow_name": None, "timestamp": "3", "details": "{}"},
        _progress("u2", "yf", "RUNNING", _dq(699774, 1), "4"),
        _progress("u2", "yf", "COMPLETED", _dq(699774, 1), "5", as_string=False),
        _progress("u2", "tiingo", "COMPLETED", _dq(78978, 0), "6"),
    ]
    out = summarise_expectations(events)
    assert out["update_id"] == "u2"
    yf = out["flows"]["yf"]
    assert yf == {"dropped_records": 0, "expectations": {"ohlc_ordered": {"passed": 699774, "failed": 1}}}
    assert out["flows"]["tiingo"]["expectations"]["ohlc_ordered"] == {"passed": 78978, "failed": 0}


def test_streaming_progress_without_a_completed_event_is_summed():
    events = [
        {"event_type": "create_update", "update_id": "u", "flow_name": None, "timestamp": "1", "details": "{}"},
        _progress("u", "st", "RUNNING", _dq(5, 1, dropped=1, name="close_positive"), "2"),
        _progress("u", "st", "RUNNING", _dq(7, 0, name="close_positive"), "3"),
    ]
    flow = summarise_expectations(events)["flows"]["st"]
    assert flow == {"dropped_records": 1, "expectations": {"close_positive": {"passed": 12, "failed": 1}}}


def test_no_data_quality_gives_no_flows():
    events = [
        {"event_type": "create_update", "update_id": "u", "flow_name": None, "timestamp": "1", "details": "{}"},
        _progress("u", "yf", "COMPLETED", None, "2"),
    ]
    assert summarise_expectations(events)["flows"] == {}
