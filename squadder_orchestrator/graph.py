"""Compatibility entrypoints for the V1 phase-driven graph builder."""

from squadder_orchestrator.graph_compiler.langgraph_builder import compile_graph


def build_graph_from_yaml(*args, **kwargs):
    """Compatibility wrapper kept for external callers."""

    return compile_graph(*args, **kwargs)
