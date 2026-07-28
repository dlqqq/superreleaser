"""Superreleaser — conda-forge release automation logic (Airflow-independent).

The DAG in ../dags/cf_release.py orchestrates these modules; everything here is
plain Python so it can be unit-tested without Airflow.
"""
