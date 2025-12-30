#!/usr/bin/env python3
"""
Validate Airflow DAGs without running full Airflow stack.
Usage: python scripts/validate_dags.py
"""
import sys
import ast
from pathlib import Path


def validate_syntax(file_path: Path) -> tuple[bool, str]:
    """Check Python syntax."""
    try:
        with open(file_path) as f:
            ast.parse(f.read())
        return True, "OK"
    except SyntaxError as e:
        return False, f"Syntax error at line {e.lineno}: {e.msg}"


def validate_dag_structure(file_path: Path) -> tuple[bool, str]:
    """Check DAG has required imports and structure."""
    content = file_path.read_text()

    issues = []

    # Check for DAG import
    if "from airflow import DAG" not in content and "from airflow.models import DAG" not in content:
        issues.append("Missing DAG import")

    # Check for DAG definition
    if "DAG(" not in content and "with DAG" not in content:
        issues.append("No DAG definition found")

    if issues:
        return False, ", ".join(issues)
    return True, "OK"


def main():
    dags_path = Path(__file__).parent.parent / "airflow" / "dags"

    if not dags_path.exists():
        print(f"DAGs directory not found: {dags_path}")
        sys.exit(1)

    dag_files = list(dags_path.glob("*.py"))

    if not dag_files:
        print("No DAG files found")
        sys.exit(1)

    print(f"Validating {len(dag_files)} DAG file(s)...\n")

    errors = []

    for dag_file in sorted(dag_files):
        print(f"  {dag_file.name}:")

        # Syntax check
        ok, msg = validate_syntax(dag_file)
        print(f"    Syntax: {msg}")
        if not ok:
            errors.append((dag_file.name, msg))
            continue

        # Structure check
        ok, msg = validate_dag_structure(dag_file)
        print(f"    Structure: {msg}")
        if not ok:
            errors.append((dag_file.name, msg))

    print()

    if errors:
        print(f"❌ {len(errors)} error(s) found:")
        for name, msg in errors:
            print(f"   - {name}: {msg}")
        sys.exit(1)
    else:
        print(f"✅ All {len(dag_files)} DAG(s) validated successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
