"""Tests for local OpenHands runtime assumptions."""

from __future__ import annotations

from openhands.tools.file_editor import FileEditorTool
from openhands.tools.terminal import TerminalTool

from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_REQUIRED_TOOL_NAMES,
)


def test_openhands_required_tool_names_match_installed_registry_names():
    assert OPENHANDS_REQUIRED_TOOL_NAMES == (
        TerminalTool.name,
        FileEditorTool.name,
    )
