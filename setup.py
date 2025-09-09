from setuptools import setup, find_packages

setup(
    name="pyLogAirflow",
    version="0.1.0",
    description="Airflow-aware logging helper (JSON logs, decorator, callbacks)",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[],
    include_package_data=True,
)
