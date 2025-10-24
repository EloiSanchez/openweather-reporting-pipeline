import os
from pyspark.sql import SparkSession
from src.destinations.adls import ADLS
from src.destinations.local_directory import LocalDirectory


spark = SparkSession.builder.getOrCreate()

local_directory = LocalDirectory("../out_raw/")
adls = ADLS(os.environ["AZURE_ACCOUNT_NAME"], os.environ["AZURE_CONTAINER_NAME"])

for location in (local_directory, adls):
    for dir in ("weather", "air_pollution"):
        tables = location.read_tables_from_dir(dir, f"main_{dir}")

        for table in tables.values():
            print(
                location.name,
                dir,
                spark.createDataFrame(
                    table.get_data(), table.get_schema(), verifySchema=False
                ).count(),
            )
