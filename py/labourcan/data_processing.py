import polars as pl
from pathlib import Path
from typing import Union


def read_labourcan(file: Union[str, Path]) -> pl.DataFrame:
    """
    Read and preprocess Labour Canada CSV data using Polars.

    This function loads labour statistics data, filters for seasonally adjusted estimates,
    and creates standardized date columns for time series analysis.

    Args:
        file (Union[str, Path]): Path to the Labour Canada CSV file to read.
            Expected to contain columns like REF_DATE, Statistics, Data type, etc.

    Returns:
        pl.DataFrame: Processed DataFrame with:
            - Unnecessary metadata columns removed
            - Filtered to seasonally adjusted estimates only
            - Additional YEAR, MONTH, and DATE_YMD columns extracted from REF_DATE
            - Sorted chronologically by year and month

    Example:
        >>> df = read_labourcan("data/14100022.csv")
        >>> print(df.columns)
        ['REF_DATE', 'GEO', 'Statistics', 'Data type', 'VALUE', 'YEAR', 'MONTH', 'DATE_YMD']

    Note:
        - Only keeps rows where Statistics == 'Estimate' and Data type == 'Seasonally adjusted'
        - REF_DATE is expected to be in format "YYYY-MM" (e.g., "2023-01")
        - Creates DATE_YMD as a proper date column (first day of each month)
    """
    return (
        # Lazy read and filter/drop unnecessary columns
        pl.scan_csv(file)
        .drop([
            "UOM_ID",           # Unit of measure ID
            "DGUID",            # Dissemination geography unique identifier
            "SCALAR_FACTOR",    # Scaling factor for values
            "SCALAR_ID",        # Scaling factor identifier
            "VECTOR",           # Statistics Canada vector identifier
            "COORDINATE",       # Coordinate reference
            "STATUS",           # Data status
            "SYMBOL",           # Data symbol
            "TERMINATED",       # Termination indicator
            "DECIMALS",         # Number of decimal places
        ])
        # Filter to seasonally adjusted estimates only
        .filter(
            pl.col("Statistics") == "Estimate",
            pl.col("Data type") == "Seasonally adjusted"
        )
        # Extract YEAR and MONTH from REF_DATE (format: YYYY-MM)
        .with_columns([
            pl.col("REF_DATE")
            .str.extract(r"^(\d{4})")
            .cast(pl.Int32)
            .alias("YEAR"),

            pl.col("REF_DATE")
            .str.extract(r"-(\d{2})$")
            .cast(pl.Int32)
            .alias("MONTH"),
        ])
        # Create proper date column (impute to first day of each month)
        .with_columns(
            pl.date(pl.col("YEAR"), pl.col("MONTH"), 1).alias("DATE_YMD")
        )
        # Sort chronologically
        .sort(["YEAR", "MONTH"])
        .collect()
    )
