import polars as pl
from pathlib import Path
from typing import Union
import polars.selectors as cs


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

        # Rename this really long column
        .rename({"North American Industry Classification System (NAICS)": "Industry"})

        # Convert VALUE to float
        .with_columns(pl.col("VALUE").cast(pl.Float64, strict=False))

        .collect()
    )


def calculate_centered_rank(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate monthly percentage changes and add centered rankings within time periods.

    This function combines monthly percentage change calculations with centered ranking
    to identify relative performance of different groups (e.g., industries) within
    each time period (year-month combination).

    Args:
        df (pl.DataFrame): Input DataFrame with labour data

    Returns:
        pl.DataFrame: DataFrame with monthly changes and centered ranks, including:
            - All columns from calculate_monthly_percent_change()
            - New ranking column "centered_rank_across_industry" showing relative performance 
              within each time period

    Interpretation of Ranks:
        - Rank 0: No change (PDIFF ≈ 0)
        - Negative ranks: Below-average performance (decreases)
            - Rank -1: Worst performer (largest decrease)
        - Positive ranks: Above-average performance (increases)  
            - Rank 1: Best performer among those with increase
    """
    intermediate = calculate_monthly_percent_change(df)
    out = intermediate.with_columns(
        centered_rank_across_industry=centered_rank_expr(pl.col("PDIFF")).over(
            ["YEAR", "MONTH"]
        )
    )
    return out


def calculate_monthly_percent_change(
    df: pl.DataFrame,
    group_by: str = "Industry"
) -> pl.DataFrame:
    """
    Calculate month-over-month percentage change for labour statistics.

    This function computes the percentage difference between consecutive months
    for each group (e.g., by Industry), providing insights into monthly growth rates.

    Args:
        df (pl.DataFrame): Input DataFrame containing labour data with columns:
            - group_by column (default "Industry")  
            - value_col (default "VALUE")
            - "YEAR", "MONTH", "DATE_YMD" for time sorting
        group_by (str): Column name to group by when calculating changes. 
            Defaults to "Industry"

    Returns:
        pl.DataFrame: DataFrame with original data plus new columns:
            - LAGGED_VALUE: Previous month's value for the same group
            - DIFF: Absolute difference (current - previous)
            - PDIFF: Percentage difference ((current - previous) / previous)

    Note:
        - First observation for each group will have null values for LAGGED_VALUE, 
          DIFF, and PDIFF since there's no previous month to compare against
        - Percentage changes are in decimal format (0.05 = 5% increase)
        - Negative PDIFF values indicate month-over-month decreases
    """
    sort_cols = [group_by, "YEAR", "MONTH"]

    return (
        # sort chronologically by time, then lag value is the month before
        # but also perform over groups
        df.sort(sort_cols)
        .with_columns(
            LAGGED_VALUE=pl.col("VALUE")
            .shift(1)
            .over(group_by)
        )
        # compute absolute and percent difference
        .with_columns((pl.col("VALUE") - pl.col("LAGGED_VALUE")).alias("DIFF"))
        .with_columns((pl.col("DIFF") / pl.col("LAGGED_VALUE")).alias("PDIFF"))
        .select(
            pl.col(group_by),
            pl.col("DATE_YMD"),
            pl.col("YEAR"),
            pl.col("MONTH"),
            cs.matches("VALUE"),
            cs.matches("DIFF"),
        )
        .sort([group_by, "YEAR", "MONTH", "PDIFF"])
    )


def centered_rank_expr(col: pl.Expr) -> pl.Expr:
    """
    Create a centered ranking expression that ranks values around zero.

    This ranking system treats zero as the center point, with negative values
    ranked negatively and positive values ranked positively.

    - Largest negative value gets rank -1
    - Smallest positive value gets rank +1
    - Zero gets rank 0

    Args:
        col (pl.Expr): Polars expression for the column to rank

    Example:
        Given values: [-0.05, -0.02, 0, 0.01, 0.03]
        Centered ranks: [-1, -2, 0, 1, 2]
        >>> df = pl.DataFrame({"values": [-0.05, -0.02, 0, 0.01, 0.03]})
        >>> df.with_columns(
        ...     centered_rank=centered_rank_expr(pl.col("values"))
        ... )

    """
    return (
        pl.when(col < 0)
        .then(
            # minus the total # of -ve values
            (col.rank(method="ordinal", descending=True) * -1) + (col > 0).sum()
        )
        .when(col == 0)
        .then(pl.lit(0))
        .when(col > 0)
        .then(col.rank(method="ordinal") - (col < 0).sum())
        .otherwise(pl.lit(None))  # Handle null values
    )
