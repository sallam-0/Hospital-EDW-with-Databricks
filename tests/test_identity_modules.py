import ast
import json
import pytest

from config.identity_config import (
    IDENTITY_TABLES,
    AUTO_MERGE_THRESHOLD,
    REVIEW_MIN_THRESHOLD,
    MAX_COMPONENT_ITERATIONS,
)


def test_identity_config():
    assert "source" in IDENTITY_TABLES
    assert "registry" in IDENTITY_TABLES
    assert "aliases" in IDENTITY_TABLES
    assert "patient_map" in IDENTITY_TABLES
    assert "dim_patient" in IDENTITY_TABLES
    assert "pair_evidence" in IDENTITY_TABLES
    assert "review_queue" in IDENTITY_TABLES
    assert "exceptions" in IDENTITY_TABLES
    assert "run_audit" in IDENTITY_TABLES

    assert 0.0 < REVIEW_MIN_THRESHOLD < AUTO_MERGE_THRESHOLD <= 1.0
    assert MAX_COMPONENT_ITERATIONS > 0


def test_notebook_syntax_and_structure():
    ipynb_path = "notebooks/identity/03_patient_identity_probabilistic_enterprise.ipynb"
    with open(ipynb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    assert "cells" in nb
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    assert len(code_cells) >= 8

    for idx, cell in enumerate(code_cells):
        src = "".join(cell["source"]).strip()
        if src.startswith("%"):
            # Ensure restartPython() is NOT called with %pip
            assert "restartPython" not in src, "dbutils.library.restartPython() must not be called with %pip"
            continue
        try:
            ast.parse(src)
        except SyntaxError as e:
            pytest.fail(f"Code cell {idx} failed syntax validation: {e}")
