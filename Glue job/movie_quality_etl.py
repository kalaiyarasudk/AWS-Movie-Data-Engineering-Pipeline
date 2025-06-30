import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import IntegerType, DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

movie_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="movies",
    table_name="movies_s3-movie_rawdata"
)
movie_df = movie_dyf.toDF()

movie_df = movie_df.withColumn("Released_Year", col("Released_Year").cast(IntegerType()))
movie_df = movie_df.withColumn("IMDB_Rating", col("IMDB_Rating").cast(DoubleType()))
movie_df = movie_df.withColumn("Meta_score", col("Meta_score").cast(IntegerType()))
movie_df = movie_df.withColumn("No_of_Votes", col("No_of_Votes").cast(IntegerType()))
movie_df = movie_df.withColumn("Gross", col("Gross").cast(DoubleType()))

movie_df = movie_df.withColumn("Runtime_Minutes", regexp_extract("Runtime", r"(\d+)", 1).cast(IntegerType()))

# Apply filters
processed_df = movie_df.filter(
    (col("Series_Title").isNotNull()) &
    (col("Released_Year").between(1888, 2025)) &
    (col("IMDB_Rating").between(7.0, 10.0)) &
    (col("Runtime_Minutes") > 0)
)

# writing the processed data to S3 in parquet format
processed_output_path = "s3://movie-quality/Processed-data/"

processed_df.write.mode("overwrite").parquet(processed_output_path)

print(f"processed data written to: {processed_output_path}")

job.commit()
print("Movie processing completed successfully.")
