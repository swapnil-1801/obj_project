!pip install pyspark
import logging
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import pyarrow.parquet as pq
import pymysql
import mysql.connector
from pyspark.sql.functions import udf, lit, col
from cryptography.fernet import Fernet
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pandera as pa
from pandera import Column, Check
from pandera import check_input
from pandera.errors import SchemaError

!pip install -q findspark
import findspark
findspark.init()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Sampl").getOrCreate()

now = datetime.now()
current_time = now.strftime("%H:%M")
print(current_time)

####### Objective 2 #######################
def obj_2(spark):
    
    # CSV --> df --> Parquet
    cust_df = spark.read.csv("dataset/customers.csv", inferSchema = True, header = True)
    cust_df.write.partitionBy("location").parquet('dataset/location_'+current_time+'/')
    
    # Parquet Dataset --> df 
    parquet_df = pq.ParquetDataset('dataset/location_'+current_time+'/').read_pandas().to_pandas()
    
    # df --> MySQL table : No need to create schema in MySQL
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="swapnil",
                               pw="swapnil123",
                               db="mysql"))

    parquet_df.to_sql('parquet_table',engine, if_exists = 'replace', chunksize = 1000)
    return parquet_df

####### Objective 3 - Casting and Masking #######################

# encrypt func
def encrypt_data(plain_text, KEY):
    f = Fernet(KEY)
    encrip_text = f.encrypt(str(plain_text).encode()).decode()
    return encrip_text


def decrypt_data(encrypt_data, KEY):
    f = Fernet(bytes(KEY))
    decoded_val = f.decrypt(encrypt_data.encode()).decode()
    return decoded_val

####### Objective - 4: Pre-validations/ Post-validations #######################

def obj_4(customers_df):
    
    city = ['New York','Berlin','Tokyo','Pune','Singapore','Paris','London','Mexico','Sydney','Rome','Mumbai','Dubai','Beijing','Chicago','Mysore','Madrid','Chennai','Hyd','Delhi','Egypt','Madras','Bombay','France']
    schema = pa.DataFrameSchema(
    {
        "customer_id": Column(int, Check.less_than(490), Check.in_range(0, 501), Check(lambda x: x.sum() > 500)),
        "customer_name": Column(str, nullable=False),
        "location": Column(str, Check.isin(city), nullable=False),
    }, ordered=True, report_duplicates = "all", unique=["customer_id"])
    
    try:
      schema(customers_df)
    except SchemaError as e:
      print("Failed check:", e.check)
      print("\n dataframe:\n", e.data)
      print("\nFailure cases:\n", e.failure_cases)
    
####### Objective - 5: SQL Operations #######################
    
def obj_5(spark):
    
    customers_df = spark.read.csv("dataset/customers.csv", inferSchema = True, header = True)
    sales_df = spark.read.csv("dataset/sales.csv", inferSchema = True, header = True)
    products_df = spark.read.csv("dataset/products.csv", inferSchema = True, header = True)
    
    # Calculate the monthly sales for each product category
    products_df.join(sales_df,["product_id"], "inner") \
        .select(month(sales_df.transaction_date).alias('month'), products_df.category, sales_df.quantity) \
        .groupBy('month','category').sum('quantity').orderBy('month').show(10)
    
    # Calculate highest selling product_name for each month
    products_df1 = products_df.join(sales_df,["product_id"], "inner") \
        .select(month(sales_df.transaction_date).alias('month'), products_df.product_name, sales_df.quantity) 
    products_df1 = products_df1.withColumn("rank",rank().over(Window.partitionBy("month").orderBy(col("quantity").desc())))    
    products_df1.where(products_df1.rank == 1).show(10)
    
    # Calculate two highest selling category for each month
    products_df2 = products_df.join(sales_df,["product_id"], "inner") \
        .select(month(sales_df.transaction_date).alias('month'), products_df.category, sales_df.quantity)
    products_df2 = products_df2.withColumn("rank",row_number().over(Window.partitionBy("month").orderBy(col("quantity").desc())))
    products_df2.where(products_df2.rank < 3).show(10)
    
    # Calculate highest sale for each month
    product_df3 = products_df.join(sales_df,["product_id"],"inner") \
        .select(month(sales_df.transaction_date).alias("month"),products_df.category, (sales_df.quantity*sales_df.price).alias("total"))
    product_df3 = product_df3.withColumn("rank",row_number().over(Window.partitionBy("month").orderBy(col("total").desc())))
    product_df3.filter(product_df3.rank == 1).show(10)
    
    # Calculate highest sale for each year
    product_df4 = products_df.join(sales_df,["product_id"], "inner") \
        .select(year(sales_df.transaction_date).alias("year"), products_df.category, (sales_df.quantity*sales_df.price).alias("total"))
    product_df4 = product_df4.withColumn("rank", row_number().over(Window.partitionBy("year").orderBy(product_df4.total.desc())))
    product_df4.where(product_df4.rank == 1).show(10)   
    

def main(spark):
    logger.info("Running obj_2...")
    #obj_2_obj = obj_2()
    parquet_df = obj_2(spark)
    logger.info("obj_2 run completed...")
    
    logger.info("Running obj_3...")
    # generate the encryption key
    Key = Fernet.generate_key()
    
    customers_df = spark.read.csv("dataset/customers.csv", inferSchema = True, header = True)
    encrypt_udf = udf(encrypt_data, StringType())
    enc_df = customers_df.withColumn("encrypted_name", encrypt_udf(col('customer_name'), lit(Key))) 
    print("\nEncrypted:\n")
    enc_df.select(['customer_id','encrypted_name','location']).show()
    
    decrypt_udf = udf(decrypt_data, StringType())
    # decrypt the data
    dec_df = enc_df.withColumn("decrypted_name", decrypt_udf(col('encrypted_name'), lit(Key)))
    print("\nDecrypted:\n")
    dec_df.select(['customer_id','decrypted_name','location']).show()
    logger.info("obj_3 run completed...")
    
    logger.info("Running obj_4...")
    customers_df = pd.read_csv("dataset/customers.csv")
    obj_4(customers_df)
    logger.info("obj_4 run completed...")
    
    logger.info("Running obj_5...")    
    obj_5(spark)
    logger.info("obj_5 run completed...")


main(spark)