from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
"""
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
#catalog_name=spark.conf.get('catalog_name')
volumne_path = "/Volumes/youtube_dev/bronze/earthquake_data/2026-06-11/"
#f"/Volumes/{catalog_name}/bronze/earthquake_data"
primary_key="id"
properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType()),
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

schema = ArrayType(feature_schema)


@dp.table(name="earthquake_data_suing_stream_df")
def earthquake_data():
    	# Sample data: 2 rows
	data = [("Alice", 25), ("Bob", 30)]
	# Define schema (column names)
	columns = ["name", "age"]
	# Create DataFrame
	df = spark.createDataFrame(data, columns)
    """
    df = spark.readStream.format("cloudfiles").option("cloudFiles.format", "json").load(volumne_path).withColumn("_load_timestamp", current_timestamp())
    """
    return df
"""