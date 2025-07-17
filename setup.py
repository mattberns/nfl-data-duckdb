from setuptools import setup, find_packages

setup(
    name="nfl_analytics",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "nfl_data_py>=0.3.2",
        "duckdb>=1.1.3",
        "pandas>=2.2.3",
        "numpy>=1.26.4",
        "pyarrow>=18.1.0",
        "tqdm>=4.66.6",
    ],
    python_requires=">=3.12",
    description="NFL data analytics system using nfl_data_py and DuckDB",
    author="NFL Analytics Team",
    author_email="team@nfl-analytics.com",
)
