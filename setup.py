"""Minimal package metadata used to stage local modules on Dataflow workers."""

from setuptools import find_packages, setup


setup(
    name="financial-risk-data-platform-streaming",
    version="1.4.0",
    description="Cost-controlled GCP streaming demo modules",
    packages=find_packages(include=["gcp_streaming*", "streaming*"]),
)
