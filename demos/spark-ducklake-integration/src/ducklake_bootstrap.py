import duckdb
import os
from loguru import logger
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, DataFrameWriter


class OdcDuckLake:
    def __init__(self, spark: SparkSession, catalog_name: str, schema_name: str):
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        load_dotenv(env_path)

        # Postgres variables
        self.pg_host = os.getenv("POSTGRES_HOST")
        self.pg_port = os.getenv("POSTGRES_PORT", "5432")
        self.pg_db = os.getenv("POSTGRES_DB")
        self.pg_user = os.getenv("POSTGRES_USER")
        self.pg_password = os.getenv("POSTGRES_PASSWORD")

        if not all([self.pg_host, self.pg_db, self.pg_user, self.pg_password]):
            raise ValueError("Postgres required environment variables are missing")

        # Minio variables
        self.mio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.mio_bucket = os.getenv("MINIO_BUCKET")
        self.mio_user = os.getenv("MINIO_USER")
        self.mio_password = os.getenv("MINIO_PASSWORD")

        if not all([self.mio_bucket, self.mio_user, self.mio_password]):
            raise ValueError("Minio required environment variables are missing")

        # Ducklake
        self.py_conn = duckdb.connect()  # Install under the default dir: /.duckdb

        self.ducklake_uri = f"postgres:host={self.pg_host} port={self.pg_port} dbname={self.pg_db} user={self.pg_user} password={self.pg_password}"
        self.ducklake_catalog = catalog_name
        self.ducklake_schema = schema_name

        # Init
        self.spark = spark
        self.init_sql = self._get_init_sql()
        self._init_ducklake()
        self._connect_ducklake_jdbc()

    def _get_init_sql(self):
        return f"""
            INSTALL httpfs;
            INSTALL postgres;
            INSTALL ducklake;

            LOAD httpfs;
            LOAD postgres;
            LOAD ducklake;

            CREATE OR REPLACE SECRET (
                TYPE postgres,
                HOST '{self.pg_host}',
                PORT {self.pg_port},
                DATABASE '{self.pg_db}',
                USER '{self.pg_user}',
                PASSWORD '{self.pg_password}'
            );
            
            CREATE OR REPLACE SECRET minio_secret (
                TYPE s3,
                KEY_ID '{self.mio_user}',
                SECRET '{self.mio_password}',
                ENDPOINT '{self.mio_endpoint}',
                USE_SSL false,
                URL_STYLE 'path'
            );
            
            ATTACH 'ducklake:postgres:dbname={self.pg_db}' AS {self.ducklake_catalog} (
                DATA_PATH 's3a://{self.mio_bucket}/'
            );
        """

    def _init_ducklake(self):
        """
        Use python DuckDB to set up postgres as the ducklake based on MinIO
        Env configures will be used
        """
        logger.info("Initializing ducklake...")
        self.py_conn.execute(self.init_sql)

        # Create schema
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.ducklake_catalog}.{self.ducklake_schema};"
        self.py_conn.execute(create_schema_sql)

        # Close the connection. DuckDB uses a lock, so multiple simultaneous connections are not allowed.
        self.py_conn.close()

    def _connect_ducklake_jdbc(self):
        """
        There 2 duckdb: Python and JDBC DuckDB
        Init JDBC DuckDB to connect ducklake created by Python DuckDB
        It's hard to apply env config to JDBC DuckDB, run a test query beforehand to make the config effect
        """
        logger.info("Connecting ducklake...")
        init_df = (
            self.spark.read
            .format("jdbc")
            .option("driver", "org.duckdb.DuckDBDriver")
            .option("url", f"jdbc:duckdb:ducklake:{self.ducklake_uri}")
            .option("dbtable", "(SELECT 1 as test) AS t")
            .option("sessionInitStatement", self.init_sql)
            .load()
        )

        init_df.collect()
        logger.info("Successfully connecting ducklake")

    def insert_seed_data(self):
        """
        Parquet file: yellow_tripdata_2025-09.parquet is from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
        Uploaded to Minio beforehand
        See: docker-compose.yml

        Create table yellow_tripdata_202509_v1 in ducklake from the parquet file
        """
        conn = duckdb.connect()
        conn.execute(self.init_sql)

        table_name = "yellow_tripdata_202509_v1"
        parquet_file = "yellow_tripdata_2025-09.parquet"
        yellow_taxi_url = f"s3a://{self.mio_bucket}/{parquet_file}"

        sql = f"""
            USE {self.ducklake_catalog}.{self.ducklake_schema};
            CREATE TABLE IF NOT EXISTS {self.ducklake_schema}.{table_name} AS 
                FROM read_parquet('{yellow_taxi_url}');
        """
        conn.execute(sql)
        conn.close()

    def get_df_writer(self, df: DataFrame) -> DataFrameWriter:
        return (
            df.write
            .format("jdbc")
            .option("driver", "org.duckdb.DuckDBDriver")
            .option('url', f'jdbc:duckdb:ducklake:{self.ducklake_uri}')
        )

    def get_df_reader(self, spark: SparkSession, table_name: str) -> DataFrame:
        return (
            spark.read
            .format("jdbc")
            .option("driver", "org.duckdb.DuckDBDriver")
            .option("url", f"jdbc:duckdb:ducklake:{self.ducklake_uri}")
            .option("dbtable", table_name)
            .load()
        )

    def get_sql_reader(self, spark: SparkSession, table_name: str, temp_view_name: str) -> SparkSession:
        # Create a temporary view of the table to enable querying it with Spark SQL
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW {temp_view_name}
            USING jdbc
            OPTIONS (
                url "jdbc:duckdb:ducklake:{self.ducklake_uri}",
                driver "org.duckdb.DuckDBDriver",
                dbtable "{self.ducklake_schema}.{table_name}"
            )
        """)
        return spark
