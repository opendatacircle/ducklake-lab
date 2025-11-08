from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, IntegerType
import pyspark.sql.functions as F

from ducklake_bootstrap import OdcDuckLake


class SparkDucklakeDemo:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("OdcSparkDucklake")
            .config('spark.driver.defaultJavaOptions', '-Djava.security.manager=allow')
            .getOrCreate()
        )

        self.catalog_name = "ducklake_catalog"
        self.schema_name = "odc_demo"
        # Init JDBC DucDB
        self.odc_dl = OdcDuckLake(self.spark, self.catalog_name, self.schema_name)

    def ingest_taxi_pickup_hour(self):
        src_table = "yellow_tripdata_202509_v1"
        dst_table = "agg_yellow_pickup_hour_202509_v1"
        # Generate table: yellow_tripdata_202509_v1
        self.odc_dl.insert_seed_data()

        # SparkSQL: Calculate pickup hour
        src_view = "yellow_tripdata_202509_view_v1"  # Temporary view of the table
        query = f"""
            SELECT
                hour(tpep_pickup_datetime) AS pickup_hour,
                COUNT(*) AS total_trips
            FROM {src_view}
            GROUP BY hour(tpep_pickup_datetime)
            ORDER BY pickup_hour;
        """

        try:
            # SQL reader
            logger.info(f"Calculating pickup hour from the table: {src_table} ...")
            df = (
                self.odc_dl
                .get_sql_reader(self.spark, src_table, src_view)
                .sql(query)
                .withColumn("pickup_hour", F.col("pickup_hour").cast(IntegerType()))
                .withColumn("total_trips", F.col("total_trips").cast(LongType()))
            )

            # Dataframe writer
            logger.info(f"Writing the result into the table: {dst_table} ")
            (
                self.odc_dl
                .get_df_writer(df)
                .option('dbtable', f"{self.schema_name}.{dst_table}")
                .mode("overwrite")
                .save()
            )

            # Dataframe reader
            logger.info(f"Sampling the table: {dst_table}")
            (
                self.odc_dl
                .get_df_reader(self.spark, f"{self.schema_name}.{dst_table}")
                .orderBy(F.col("total_trips").desc())
                .limit(5)
                .show()
            )

        except Exception as e:
            logger.error(f"Failed to process the pickup hour of the table: {src_table}: {e}")
            raise e

if __name__ == "__main__":
    demo = SparkDucklakeDemo()
    demo.ingest_taxi_pickup_hour()

