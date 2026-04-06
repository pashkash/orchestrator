from __future__ import annotations

from start_oh_server import _disable_local_filestore_tool_spans


def _wrap(fn):
    def wrapped(*args, **kwargs):  # noqa: ANN002, ANN003
        return fn(*args, **kwargs)

    wrapped.__wrapped__ = fn
    return wrapped


class _FakeLocalFileStore:
    @_wrap
    def write(self, path, contents):  # noqa: ANN001, ANN201
        return ("write", path, contents)

    @_wrap
    def list(self, path):  # noqa: ANN001, ANN201
        return ["before", path]

    @_wrap
    def delete(self, path):  # noqa: ANN001, ANN201
        return ("delete", path)


def test_disable_local_filestore_tool_spans_unwraps_observed_methods() -> None:
    _disable_local_filestore_tool_spans(_FakeLocalFileStore)

    store = _FakeLocalFileStore()
    assert not hasattr(_FakeLocalFileStore.write, "__wrapped__")
    assert not hasattr(_FakeLocalFileStore.list, "__wrapped__")
    assert not hasattr(_FakeLocalFileStore.delete, "__wrapped__")
    assert store.write("a.txt", "x") == ("write", "a.txt", "x")
    assert store.list("dir") == ["before", "dir"]
    assert store.delete("a.txt") == ("delete", "a.txt")
