from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import glob
import psycopg2


class ETLPipeline:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.setup_postgres_tables()

    def create_spark_session(self):
        spark = SparkSession.builder \
            .appName("ETL_Pipeline_Star_Schema") \
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.7.1,"
                    "com.clickhouse:clickhouse-jdbc:0.6.3") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        return spark

    def setup_postgres_tables(self):
        try:
            conn = psycopg2.connect(
                host="postgres-db",
                database="postgres",
                user="postgres",
                password="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()

            drop_tables = [
                "DROP TABLE IF EXISTS fact_sale CASCADE",
                "DROP TABLE IF EXISTS dim_category CASCADE",
                "DROP TABLE IF EXISTS dim_pet CASCADE",
                "DROP TABLE IF EXISTS dim_customer CASCADE",
                "DROP TABLE IF EXISTS dim_product CASCADE",
                "DROP TABLE IF EXISTS dim_seller CASCADE",
                "DROP TABLE IF EXISTS dim_store CASCADE",
                "DROP TABLE IF EXISTS dim_supplier CASCADE",
                "DROP TABLE IF EXISTS mock_data CASCADE"
            ]

            for drop in drop_tables:
                try:
                    cursor.execute(drop)
                    print(f"Executed: {drop[:50]}...")
                except Exception as e:
                    print(f"Warning on drop: {e}")

            cursor.execute("""
                CREATE TABLE mock_data (
                    id INTEGER,
                    customer_first_name VARCHAR(255),
                    customer_last_name VARCHAR(255),
                    customer_age INTEGER,
                    customer_email VARCHAR(255),
                    customer_country VARCHAR(255),
                    customer_postal_code VARCHAR(50),
                    customer_pet_type VARCHAR(100),
                    customer_pet_name VARCHAR(100),
                    customer_pet_breed VARCHAR(100),
                    seller_first_name VARCHAR(255),
                    seller_last_name VARCHAR(255),
                    seller_email VARCHAR(255),
                    seller_country VARCHAR(255),
                    seller_postal_code VARCHAR(50),
                    product_name VARCHAR(255),
                    product_category VARCHAR(100),
                    product_price NUMERIC(10,2),
                    product_quantity INTEGER,
                    sale_date VARCHAR(50),
                    sale_customer_id INTEGER,
                    sale_seller_id INTEGER,
                    sale_product_id INTEGER,
                    sale_quantity INTEGER,
                    sale_total_price NUMERIC(12,2),
                    store_name VARCHAR(255),
                    store_location VARCHAR(255),
                    store_city VARCHAR(100),
                    store_state VARCHAR(100),
                    store_country VARCHAR(255),
                    store_phone VARCHAR(50),
                    store_email VARCHAR(255),
                    pet_category VARCHAR(100),
                    product_weight NUMERIC(8,2),
                    product_color VARCHAR(100),
                    product_size VARCHAR(50),
                    product_brand VARCHAR(100),
                    product_material VARCHAR(100),
                    product_description TEXT,
                    product_rating NUMERIC(3,2),
                    product_reviews INTEGER,
                    product_release_date VARCHAR(50),
                    product_expiry_date VARCHAR(50),
                    supplier_name VARCHAR(255),
                    supplier_contact VARCHAR(255),
                    supplier_email VARCHAR(255),
                    supplier_phone VARCHAR(50),
                    supplier_address TEXT,
                    supplier_city VARCHAR(100),
                    supplier_country VARCHAR(255)
                )
            """)
            print("Table mock_data created successfully")

            create_tables = [
                """
                CREATE TABLE IF NOT EXISTS dim_category (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(100) NOT NULL UNIQUE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_pet (
                    id SERIAL PRIMARY KEY,
                    pet_type VARCHAR(100),
                    pet_name VARCHAR(100),
                    pet_breed VARCHAR(100),
                    customer_email VARCHAR(255) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_customer (
                    id SERIAL PRIMARY KEY,
                    customer_first_name VARCHAR(255),
                    customer_last_name VARCHAR(255),
                    customer_age INTEGER,
                    customer_email VARCHAR(255) NOT NULL UNIQUE,
                    customer_country VARCHAR(255),
                    customer_postal_code VARCHAR(50)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_product (
                    id SERIAL PRIMARY KEY,
                    product_name VARCHAR(255),
                    product_category VARCHAR(100),
                    product_price NUMERIC(10,2),
                    product_quantity INTEGER,
                    product_weight NUMERIC(8,2),
                    product_color VARCHAR(100),
                    product_size VARCHAR(50),
                    product_brand VARCHAR(100),
                    product_material VARCHAR(100),
                    product_description TEXT,
                    product_rating NUMERIC(3,2),
                    product_reviews INTEGER,
                    product_release_date DATE,
                    product_expiry_date DATE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_seller (
                    id SERIAL PRIMARY KEY,
                    seller_first_name VARCHAR(255),
                    seller_last_name VARCHAR(255),
                    seller_email VARCHAR(255) NOT NULL UNIQUE,
                    seller_country VARCHAR(255),
                    seller_postal_code VARCHAR(50)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_store (
                    id SERIAL PRIMARY KEY,
                    store_name VARCHAR(255),
                    store_location VARCHAR(255),
                    store_city VARCHAR(100),
                    store_state VARCHAR(100),
                    store_country VARCHAR(255),
                    store_phone VARCHAR(50),
                    store_email VARCHAR(255) NOT NULL UNIQUE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_supplier (
                    id SERIAL PRIMARY KEY,
                    supplier_name VARCHAR(255),
                    supplier_contact VARCHAR(255),
                    supplier_email VARCHAR(255) NOT NULL UNIQUE,
                    supplier_phone VARCHAR(50),
                    supplier_address TEXT,
                    supplier_city VARCHAR(100),
                    supplier_country VARCHAR(255)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS fact_sale (
                    id SERIAL PRIMARY KEY,
                    sale_date DATE,
                    sale_quantity INTEGER,
                    sale_total_price NUMERIC(12,2),
                    category_id INTEGER REFERENCES dim_category(id),
                    pet_id INTEGER REFERENCES dim_pet(id),
                    customer_id INTEGER REFERENCES dim_customer(id),
                    product_id INTEGER REFERENCES dim_product(id),
                    seller_id INTEGER REFERENCES dim_seller(id),
                    store_id INTEGER REFERENCES dim_store(id),
                    supplier_id INTEGER REFERENCES dim_supplier(id)
                )
                """
            ]

            for create in create_tables:
                cursor.execute(create)
                print("Table created successfully")

            cursor.close()
            conn.close()
            print("All tables created successfully!")

        except Exception as e:
            print(f"Error setting up tables: {e}")

    def read_mock_data(self):
        csv_files = glob.glob('/mock_data/*.csv')

        if not csv_files:
            raise Exception("No data files found. Please check CSV files are mounted correctly.")

        print(f"Found {len(csv_files)} CSV files")

        dfs = []
        for file_path in csv_files:
            try:
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("nullValue", "") \
                    .option("multiLine", "true") \
                    .option("escape", "\"") \
                    .csv(file_path)
                dfs.append(df)
                print(f"Successfully read {file_path} with {df.count()} rows")
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

        if dfs:
            final_df = dfs[0]
            for df in dfs[1:]:
                final_df = final_df.unionAll(df)
            print(f"Total records: {final_df.count()}")
            return final_df
        else:
            raise Exception("No data could be read from CSV files")

    def transform_and_load_star_schema(self, df):
        pg_url = "jdbc:postgresql://postgres-db:5432/postgres"
        pg_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

        print("Loading data into mock_data table...")
        df.write.jdbc(url=pg_url, table="mock_data", mode="append", properties=pg_properties)
        print(f"Loaded {df.count()} records into mock_data")

        df.createOrReplaceTempView("mock_data")

        print("Loading dim_category...")
        dim_category = self.spark.sql("""
            SELECT DISTINCT pet_category AS category
            FROM mock_data
        """)
        dim_category.write.jdbc(url=pg_url, table="dim_category", mode="append", properties=pg_properties)
        dim_category_with_id = self.spark.read.jdbc(url=pg_url, table="dim_category", properties=pg_properties)
        dim_category_with_id.createOrReplaceTempView("dim_category")

        print("Loading dim_pet...")
        dim_pet = self.spark.sql("""
            SELECT DISTINCT
                customer_pet_type, customer_pet_name, customer_pet_breed, customer_email
            FROM mock_data
        """)
        dim_pet = dim_pet.select(
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed"),
            col("customer_email")
        )
        dim_pet.write.jdbc(url=pg_url, table="dim_pet", mode="append", properties=pg_properties)
        dim_pet_with_id = self.spark.read.jdbc(url=pg_url, table="dim_pet", properties=pg_properties)
        dim_pet_with_id.createOrReplaceTempView("dim_pet")

        print("Loading dim_customer...")
        dim_customer = self.spark.sql("""
            SELECT DISTINCT
                customer_first_name, customer_last_name, customer_age,
                customer_email, customer_country, customer_postal_code
            FROM mock_data
        """)
        dim_customer.write.jdbc(url=pg_url, table="dim_customer", mode="append", properties=pg_properties)
        dim_customer_with_id = self.spark.read.jdbc(url=pg_url, table="dim_customer", properties=pg_properties)
        dim_customer_with_id.createOrReplaceTempView("dim_customer")

        print("Loading dim_product...")
        dim_product = self.spark.sql("""
            SELECT DISTINCT
                product_name, product_category, product_price, product_quantity,
                product_weight, product_color, product_size, product_brand,
                product_material, product_description, product_rating, product_reviews,
                CASE
                    WHEN product_release_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                    THEN TO_DATE(product_release_date, 'MM/dd/yyyy')
                END AS product_release_date,
                CASE
                    WHEN product_expiry_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                    THEN TO_DATE(product_expiry_date, 'MM/dd/yyyy')
                END AS product_expiry_date
            FROM mock_data
        """)
        dim_product.write.jdbc(url=pg_url, table="dim_product", mode="append", properties=pg_properties)
        dim_product_with_id = self.spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_properties)
        dim_product_with_id.createOrReplaceTempView("dim_product")

        print("Loading dim_seller...")
        dim_seller = self.spark.sql("""
            SELECT DISTINCT
                seller_first_name, seller_last_name, seller_email,
                seller_country, seller_postal_code
            FROM mock_data
        """)
        dim_seller.write.jdbc(url=pg_url, table="dim_seller", mode="append", properties=pg_properties)
        dim_seller_with_id = self.spark.read.jdbc(url=pg_url, table="dim_seller", properties=pg_properties)
        dim_seller_with_id.createOrReplaceTempView("dim_seller")

        print("Loading dim_store...")
        dim_store = self.spark.sql("""
            SELECT DISTINCT
                store_name, store_location, store_city, store_state,
                store_country, store_phone, store_email
            FROM mock_data
        """)
        dim_store.write.jdbc(url=pg_url, table="dim_store", mode="append", properties=pg_properties)
        dim_store_with_id = self.spark.read.jdbc(url=pg_url, table="dim_store", properties=pg_properties)
        dim_store_with_id.createOrReplaceTempView("dim_store")

        print("Loading dim_supplier...")
        dim_supplier = self.spark.sql("""
            SELECT DISTINCT
                supplier_name, supplier_contact, supplier_email,
                supplier_phone, supplier_address, supplier_city, supplier_country
            FROM mock_data
        """)
        dim_supplier.write.jdbc(url=pg_url, table="dim_supplier", mode="append", properties=pg_properties)
        dim_supplier_with_id = self.spark.read.jdbc(url=pg_url, table="dim_supplier", properties=pg_properties)
        dim_supplier_with_id.createOrReplaceTempView("dim_supplier")

        print("Loading fact_sale...")
        fact_sale = self.spark.sql("""
            SELECT
                CASE
                    WHEN md.sale_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                    THEN TO_DATE(md.sale_date, 'MM/dd/yyyy')
                END AS sale_date,
                md.sale_quantity,
                md.sale_total_price,
                c.id AS category_id,
                p.id AS pet_id,
                cust.id AS customer_id,
                prod.id AS product_id,
                s.id AS seller_id,
                st.id AS store_id,
                sup.id AS supplier_id
            FROM mock_data md
            LEFT JOIN dim_category c ON c.category = md.pet_category
            LEFT JOIN dim_pet p ON p.pet_type = md.customer_pet_type
                AND p.pet_name = md.customer_pet_name
                AND p.pet_breed = md.customer_pet_breed
                AND p.customer_email = md.customer_email
            LEFT JOIN dim_customer cust ON cust.customer_email = md.customer_email
            LEFT JOIN dim_product prod ON 
                prod.product_name = md.product_name
                AND prod.product_category = md.product_category
                AND prod.product_price = md.product_price
                AND prod.product_quantity = md.product_quantity
                AND prod.product_weight = md.product_weight
                AND prod.product_color = md.product_color
                AND prod.product_size = md.product_size
                AND prod.product_brand = md.product_brand
                AND prod.product_material = md.product_material
                AND prod.product_description = md.product_description
                AND prod.product_rating = md.product_rating
                AND prod.product_reviews = md.product_reviews
                AND prod.product_release_date = (
                    CASE WHEN md.product_release_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN TO_DATE(md.product_release_date, 'MM/dd/yyyy')
                    END)
                AND prod.product_expiry_date = (
                    CASE WHEN md.product_expiry_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN TO_DATE(md.product_expiry_date, 'MM/dd/yyyy')
                    END)
            LEFT JOIN dim_seller s ON s.seller_email = md.seller_email
            LEFT JOIN dim_store st ON st.store_email = md.store_email
            LEFT JOIN dim_supplier sup ON sup.supplier_email = md.supplier_email
        """)

        fact_sale.write.jdbc(url=pg_url, table="fact_sale", mode="append", properties=pg_properties)

        print("Star schema loaded")
