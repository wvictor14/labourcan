
def helloworld():
    return ('hello')


def read_labourcan(file):
    (
        # lazy read in and filter / drop columns
        pl.scan_csv(file)
        .drop(
            [
                "UOM_ID",
                "DGUID",
                "SCALAR_FACTOR",
                "SCALAR_ID",
                "VECTOR",
                "COORDINATE",
                "STATUS",
                "SYMBOL",
                "TERMINATED",
                "DECIMALS",
            ]
        )
        .filter(
            pl.col("GEO") == "Canada",
            pl.col('Statistics') == 'Estimate',
            ~pl.col('North American Industry Classification System (NAICS)').is_in([
            ]),
            pl.col('Data type') == 'Seasonally adjusted'
        )
        .drop(
            [
                # 'Labour force characteristics',
                "GEO",
                # "UOM",
            ]
        )
        .with_columns(pl.col("VALUE").replace("", "0.0").cast(pl.Float64))
        # define Date columns
        .with_columns(
            pl.col("REF_DATE").str.extract(
                r"^(\d+)").cast(pl.Int32).alias("YEAR"),
            pl.col("REF_DATE").str.extract(
                r"(\d+)$").cast(pl.Int32).alias("MONTH"),
        )
        .with_columns(pl.date(pl.col("YEAR"), pl.col("MONTH"), 1).alias("DATE_YMD"))
        .sort(["YEAR", "MONTH"])
        .collect()
    )
