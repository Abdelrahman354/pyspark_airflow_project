from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.functions import *
import logging
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class GoldLayer:
    def __init__(self):
        # ----------------- Spark Session --------------------
        self.spark = SparkSession.builder \
            .appName("Olist Gold Layer") \
            .master("spark://spark-master:7077") \
            .config("spark.hadoop.fs.permissions.umask-mode", "000") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.silver = "/usr/local/airflow/include/silver_layer"

        # ----------- PostgreSQL Connection --------
        self.pg_url = "jdbc:postgresql://my_postgres:5432/airflow_dw"
        self.pg_properties = {
            "user": "abdo",
            "password": "abdo",
            "driver": "org.postgresql.Driver"
        }

        logger.info("Spark session started for Gold Layer")

    # ----------------- Test PostgreSQL Connection -----------------
    def test_postgres_connection(self, ):
        try:
            test_df = self.spark.read.jdbc(
                url=self.pg_url,
                table="(SELECT 1 AS test_connection) AS t",
                properties=self.pg_properties
            )
            logger.info("PostgreSQL connection successful!")
            test_df.show()
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise

    # ----------------- Execute SQL Command -----------------
    def execute_sql(self, sql_command):
        """Execute SQL command directly on PostgreSQL using psycopg2"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="my_postgres",
                port=5432,
                database="airflow_dw",
                user=self.pg_properties["user"],
                password=self.pg_properties["password"]
            )
            cur = conn.cursor()
            cur.execute(sql_command)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("SQL executed successfully")
        except Exception as e:
            logger.error(f"SQL execution failed: {e}\nQuery: {sql_command}")
            # Do not raise error if PK/FK already exists
            if "already exists" not in str(e):
                raise

    # ----------------- Read Silver Parquet -----------------
    def read_silver(self, name):
        try:
            path = f"{self.silver}/{name}"
            df = self.spark.read.parquet(path)
            logger.info(f"Read {name}: {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to read {name} from silver layer: {e}")
            raise

    # -------------------- Build dim_customers (with geolocation) ------------------
    def build_dim_customers(self, df_customers, df_geolocation):
        df_geo_clean = df_geolocation \
            .filter(col("zip_prefix").isNotNull()) \
            .dropDuplicates(["zip_prefix"]) \
            .select("zip_prefix", "latitude", "longitude", 
                    col("city").alias("geo_city"), 
                    col("state").alias("geo_state"))
        
        dim_customers = (
            df_customers.alias("c")
            .join(
                df_geo_clean.alias("g"),
                col("c.zip_code") == col("g.zip_prefix"),
                "left"
            )
            # Modification: Use customer_id as customer_key (Business Key)
            .withColumn("customer_key", col("c.customer_id"))
            .select(
                "customer_key",
                col("c.customer_id").alias("customer_id_original"),
                col("c.customer_unique_id").alias("customer_unique_id"),
                col("c.zip_code").alias("zip_code"),
                col("c.city").alias("customer_city"),
                col("c.state").alias("customer_state"),
                col("g.latitude").cast(DoubleType()).alias("latitude"),
                col("g.longitude").cast(DoubleType()).alias("longitude"),
                col("g.geo_city").alias("geo_city"),
                col("g.geo_state").alias("geo_state")
            )
        )

        self.write_to_postgres(dim_customers, "dim_customers")

        try:
            self.execute_sql("""
                ALTER TABLE dim_customers 
                ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_key);
            """)
        except Exception:
            logger.warning("Could not add PK to dim_customers (might already exist).")

        logger.info(f"dim_customers (with geolocation) created: {dim_customers.count()} rows")
        return dim_customers

    # -------------------- Build dim_products ------------------
    def build_dim_products(self, df_products):
        dim_products = df_products.withColumn("product_key", col("product_id"))
        dim_products = dim_products.select(
            "product_key", 
            col("product_id").alias("product_id_original"), 
            "product_category", 
            col("product_weight_kg").cast(DoubleType()),
            col("product_volume_cm3").cast(DoubleType())
        )
        self.write_to_postgres(dim_products, "dim_products")

        try:
            self.execute_sql("""
                ALTER TABLE dim_products 
                ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_key);
            """)
        except Exception:
            logger.warning("Could not add PK to dim_products (might already exist).")

        logger.info(f"dim_products created: {dim_products.count()} rows")
        return dim_products

    # -------------------- Build dim_sellers (with geolocation) ------------------
    def build_dim_sellers(self, df_sellers, df_geolocation):
        df_geo_clean = df_geolocation \
            .filter(col("zip_prefix").isNotNull()) \
            .dropDuplicates(["zip_prefix"]) \
            .select("zip_prefix", "latitude", "longitude")
        
        dim_sellers = (
            df_sellers.alias("s")
            .join(
                df_geo_clean.alias("g"),
                col("s.zip_code") == col("g.zip_prefix"),
                "left"
            )
         
            .withColumn("seller_key", col("s.seller_id"))
            .select(
                "seller_key", 
                col("s.seller_id").alias("seller_id_original"), 
                "s.zip_code", 
                "s.city", 
                "s.state",
                col("g.latitude").cast(DoubleType()).alias("latitude"),
                col("g.longitude").cast(DoubleType()).alias("longitude")
            )
        )

        self.write_to_postgres(dim_sellers, "dim_sellers")

        try:
            self.execute_sql("""
                ALTER TABLE dim_sellers 
                ADD CONSTRAINT pk_dim_sellers PRIMARY KEY (seller_key);
            """)
        except Exception:
            logger.warning("Could not add PK to dim_sellers (might already exist).")

        logger.info(f"dim_sellers (with geolocation) created: {dim_sellers.count()} rows")
        return dim_sellers

    # -------------------- Build dim_date ------------------
    def build_dim_date(self, df_orders):
        df_dates = df_orders.select(
            to_date("order_purchase_timestamp").alias("full_date")
        ).dropna().distinct()
        
  
        dim_date = df_dates.withColumn(
                "date_key", 
                row_number().over(Window.orderBy("full_date")) 
            ) \
            .withColumn("year", year(col("full_date")).cast(IntegerType())) \
            .withColumn("month", month(col("full_date")).cast(IntegerType())) \
            .withColumn("day", dayofmonth(col("full_date")).cast(IntegerType())) \
            .withColumn("quarter", quarter(col("full_date")).cast(IntegerType())) \
            .withColumn("day_of_week", dayofweek(col("full_date")).cast(IntegerType())) \
            .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
            .withColumn("is_weekend", when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False)))
        
        self.write_to_postgres(dim_date, "dim_date")

        try:
            self.execute_sql("""
                ALTER TABLE dim_date 
                ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);
            """)
        except Exception:
            logger.warning("Could not add PK to dim_date (might already exist).")

        logger.info(f"dim_date created: {dim_date.count()} rows")
        return dim_date

    # -------------------- Build fact_sales--------------------
    def build_fact_sales(self, dfs, dim_customers, dim_products, dim_sellers, dim_date):
        df_orders, df_order_items, df_payments, df_reviews = dfs

        df_order_metrics = (
            df_orders.alias("o")
            .join(df_reviews.alias("r"), "order_id", "left")
            .join(df_payments.alias("p"), "order_id", "left")
            .select(
                col("o.order_id"),
                col("o.customer_id"),
                to_date(col("o.order_purchase_timestamp")).alias("order_date"),
                to_date(col("o.order_delivered_customer_date")).alias("delivered_date"),
                to_date(col("o.order_estimated_delivery_date")).alias("estimated_date"),
                
                col("o.order_purchase_timestamp"),
                col("o.order_delivered_customer_date"),
                
                col("p.payment_value").cast(DoubleType()).alias("payment_value_item"),
                col("p.payment_type"),
                col("r.review_score").cast(IntegerType()).alias("review_score_item")
            )
        
            .groupBy(
                "order_id", 
                "customer_id", 
                "order_date",
                "delivered_date",
                "estimated_date", 
                "order_purchase_timestamp", 
                "order_delivered_customer_date" 
            ).agg(
                sum("payment_value_item").alias("total_order_value"),
                concat_ws(", ", collect_set("payment_type")).alias("payment_types"),
                avg("review_score_item").cast(DoubleType()).alias("avg_review_score")
            
            )
        )

       
        fact_sales = (
            df_order_items.alias("oi")
            .join(df_order_metrics.alias("om"), "order_id", "inner")
            .select(
                col("oi.order_id"),
                col("om.customer_id"),
                col("oi.product_id"),
                col("oi.seller_id"),
                col("om.order_date"),
                
                col("om.delivered_date"),
                col("om.estimated_date"),
                col("om.order_purchase_timestamp"),
                col("om.order_delivered_customer_date"),
                
                col("oi.price").cast(DoubleType()).alias("item_price"),
                col("oi.freight_value").cast(DoubleType()).alias("item_freight_value"),
                col("om.total_order_value"),
                col("om.avg_review_score"),
                col("om.payment_types")
            )
        )

        # 3. Use Business Keys as Surrogate Keys (renaming for FK join)
        fact_sales = fact_sales \
            .withColumnRenamed("customer_id", "customer_key") \
            .withColumnRenamed("product_id", "product_key") \
            .withColumnRenamed("seller_id", "seller_key")
        
        # Join with dim_date to get the date_key (using the full_date column)
        fact_sales = fact_sales \
            .join(
                dim_date.select("date_key", col("full_date").alias("order_date_dim")), 
                col("order_date") == col("order_date_dim"),
                "left"
            ).drop("order_date_dim", "order_date")


        # 4. Select final columns and create Fact Key
        fact_sales = fact_sales.withColumn("fact_key", monotonically_increasing_id())

        fact_sales = fact_sales.select(
            "fact_key", 
            "order_id",
            "customer_key", 
            "product_key", 
            "seller_key", 
            "date_key",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "delivered_date",
            "estimated_date",
            
            "item_price", 
            "item_freight_value", 
            "total_order_value", 
            "avg_review_score",
            "payment_types"
        )
        
        self.write_to_postgres(fact_sales, "fact_sales")

        try:
            self.execute_sql("""
                ALTER TABLE fact_sales 
                ADD CONSTRAINT pk_fact_sales PRIMARY KEY (fact_key);
            """)
        except Exception:
            logger.warning("Could not add PK to fact_sales (might already exist).")

        # Add all FKs
        try:
            self.execute_sql("""
                ALTER TABLE fact_sales 
                    ADD CONSTRAINT fk_customer 
                    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
                    ADD CONSTRAINT fk_product 
                    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
                    ADD CONSTRAINT fk_seller 
                    FOREIGN KEY (seller_key) REFERENCES dim_sellers(seller_key),
                    ADD CONSTRAINT fk_date 
                    FOREIGN KEY (date_key) REFERENCES dim_date(date_key);
            """)
        except Exception:
            logger.warning("Could not add FK (might already exist).")

        logger.info(f"Fact table fact_sales created: {fact_sales.count()} rows")
        return fact_sales

    # -------------------- Write to PostgreSQL -------------------
    def write_to_postgres(self, df, table_name):
        try:
            logger.info(f"⬆ Writing {table_name} to PostgreSQL...")
            logger.info(f"Schema for {table_name}:")
            df.printSchema()
            logger.info(f"Sample data for {table_name}:")
            df.show(5, truncate=False)
            
            df.write.jdbc(
                url=self.pg_url,
                table=table_name,
                mode="overwrite",
                properties=self.pg_properties
            )
            logger.info(f"{table_name} written successfully to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {e}")
            raise

    # ------------------ Run Full Pipeline --------------------
    def run(self):
        logger.info("Starting Gold Layer transformation...")

        self.test_postgres_connection()

        # Read from Silver
        df_customers = self.read_silver("customers")
        df_products = self.read_silver("products")
        df_sellers = self.read_silver("sellers")
        df_orders = self.read_silver("orders")
        df_order_items = self.read_silver("order_items")
        df_payments = self.read_silver("payments")
        df_reviews = self.read_silver("reviews")
        df_geolocation = self.read_silver("geolocation")

        # Build Dimensions
        dim_customers = self.build_dim_customers(df_customers, df_geolocation)
        dim_products = self.build_dim_products(df_products)
        dim_sellers = self.build_dim_sellers(df_sellers, df_geolocation)
        dim_date = self.build_dim_date(df_orders)

        # Build Fact Table
        self.build_fact_sales(
            [df_orders, df_order_items, df_payments, df_reviews], 
            dim_customers, dim_products, dim_sellers, dim_date
        )

        logger.info("Gold Layer (Star Schema) built successfully with PK/FK constraints!")

    def close(self):
        self.spark.stop()
        logger.info("Spark session closed")


# ------------------ Main --------------------
if __name__ == "__main__":
    gold = GoldLayer()
    try:
        gold.run()
    finally:
        gold.close()
