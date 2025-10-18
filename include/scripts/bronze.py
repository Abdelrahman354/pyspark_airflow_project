import os
import logging
from pyspark.sql import SparkSession

os.system("chmod -R 777 /usr/local/airflow/include/bronze_layer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main():
    spark = SparkSession.builder \
        .appName("Bronze Layer Extraction") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()

    input_dir = "/usr/local/airflow/include/raw_data"
    output_dir = "/usr/local/airflow/include/bronze_layer"

    try:
        logging.info("✅ Starting Bronze extraction...")

        # Loop through all CSV files in raw directory
        for file in os.listdir(input_dir):
            if file.endswith(".csv"):
                file_path = os.path.join(input_dir, file)
                table_name = os.path.splitext(file)[0]
                output_path = os.path.join(output_dir, table_name)

                logging.info(f"Processing file: {file_path}")

                # Read CSV file
                df = spark.read.csv(file_path, header=True, inferSchema=True ,encoding="UTF-8" )
                row_count = df.count()
                logging.info(f"Loaded {row_count} rows from {file_path}")

                # Write as Parquet in Bronze layer
                df.write.mode("overwrite").parquet(output_path)

                logging.info(f"Bronze data for {table_name} written to {output_path}")

        logging.info("All raw files ingested successfully into Bronze Layer.")

    except Exception as e:
        logging.error(f"Bronze extraction failed: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":

    main()
