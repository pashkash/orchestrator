"""OpenHands HTTP polling behavior tests."""

from __future__ import annotations

from workflow_runtime.integrations.openhands_http_api import OpenHandsHttpApi


def test_wait_until_finished_uses_backoff_and_quiet_polling(monkeypatch):  # noqa: ANN001
    api = OpenHandsHttpApi(
        "http://example.invalid",
        timeout_seconds=40,
        poll_interval_seconds=2,
        max_poll_interval_seconds=7,
        poll_log_every_n_attempts=3,
    )
    states = iter(
        [
            {"execution_status": "RUNNING", "updated_at": "t1"},
            {"execution_status": "RUNNING", "updated_at": "t1"},
            {"execution_status": "RUNNING", "updated_at": "t1"},
            {"execution_status": "RUNNING", "updated_at": "t1"},
            {"execution_status": "FINISHED", "updated_at": "t2"},
        ]
    )
    observed_log_flags: list[bool] = []
    fake_clock = {"now": 0.0}
    sleep_durations: list[int] = []

    def fake_get_conversation(conversation_id, *, trace_id=None, log_reads=True):  # noqa: ANN001
        assert conversation_id == "conv-1"
        assert trace_id == "trace-1"
        observed_log_flags.append(log_reads)
        return next(states)

    def fake_monotonic() -> float:
        return fake_clock["now"]

    def fake_sleep(seconds: int) -> None:
        sleep_durations.append(seconds)
        fake_clock["now"] += seconds

    monkeypatch.setattr(api, "get_conversation", fake_get_conversation)
    monkeypatch.setattr("workflow_runtime.integrations.openhands_http_api.time.monotonic", fake_monotonic)
    monkeypatch.setattr("workflow_runtime.integrations.openhands_http_api.time.sleep", fake_sleep)

    state = api.wait_until_finished("conv-1", trace_id="trace-1")

    assert state["execution_status"] == "FINISHED"
    assert observed_log_flags == [False, False, False, False, False]
    assert sleep_durations == [2, 4, 7, 7]


def test_wait_until_finished_resets_timeout_when_conversation_progresses(monkeypatch):  # noqa: ANN001
    api = OpenHandsHttpApi(
        "http://example.invalid",
        timeout_seconds=5,
        poll_interval_seconds=2,
        max_poll_interval_seconds=4,
        poll_log_every_n_attempts=3,
    )
    states = iter(
        [
            {"execution_status": "RUNNING", "updated_at": "t1"},
            {"execution_status": "RUNNING", "updated_at": "t2"},
            {"execution_status": "RUNNING", "updated_at": "t3"},
            {"execution_status": "FINISHED", "updated_at": "t4"},
        ]
    )
    fake_clock = {"now": 0.0}
    sleep_durations: list[int] = []

    def fake_get_conversation(conversation_id, *, trace_id=None, log_reads=True):  # noqa: ANN001
        assert conversation_id == "conv-2"
        assert trace_id == "trace-2"
        assert log_reads is False
        return next(states)

    def fake_monotonic() -> float:
        return fake_clock["now"]

    def fake_sleep(seconds: int) -> None:
        sleep_durations.append(seconds)
        fake_clock["now"] += seconds

    monkeypatch.setattr(api, "get_conversation", fake_get_conversation)
    monkeypatch.setattr("workflow_runtime.integrations.openhands_http_api.time.monotonic", fake_monotonic)
    monkeypatch.setattr("workflow_runtime.integrations.openhands_http_api.time.sleep", fake_sleep)

    state = api.wait_until_finished("conv-2", trace_id="trace-2")

    assert state["execution_status"] == "FINISHED"
    assert sleep_durations == [2, 4, 4]
