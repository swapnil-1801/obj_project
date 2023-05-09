
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import pymysql

cust_df = spark.read.csv("dataset/customers.csv", inferSchema = True, header = True)

# Read single parquet into df
import pandas as pd
pq_df = pd.read_parquet('dataset/location/location=Pune/part-00000-4e79be3e-6b41-4818-b9f8-6a8ae7990d1e.c000.snappy.parquet', engine='pyarrow')

from sqlalchemy import create_engine
import pymysql
import pandas as pd
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="swapnil",
                               pw="swapnil123",
                               db="mysql"))
#dbConnection    = engine.connect()
pq_df.to_sql('pq_pune',engine, if_exists = 'append', chunksize = 1000)
------------------------------------------------------------------------------

# Parquet Dataset --> df --> MySQL table
import pyarrow.parquet as pq
parquet_df = pq.ParquetDataset('dataset/location/').read_pandas().to_pandas()
parquet_df.info(verbose=True)

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="swapnil",
                               pw="swapnil123",
                               db="mysql"))
#dbConnection    = engine.connect()
parquet_df.to_sql('parquet_table',engine, if_exists = 'append', chunksize = 1000)
--------------------------------------------------------------------------------

########### Casting- Masking ###########################
# using repartition
mask_df = spark.read.csv("dataset/cast_mask.csv", inferSchema = True, header = True)
mask_df.repartition(5).write.partitionBy("location").parquet("dataset/cast_mask)

from pyspark.sql.types import *
import pyspark.sql.functions as F

parquet_df1 = spark.read.parquet("dataset/cast_mask/")

# This is assuming your card number is not a string. If not skip this cast
parquet_cast = parquet_df1.withColumn("credit_card",F.col('credit_card').cast(StringType()))
parquet_mask = parquet_cast.withColumn("masked_credit_card",F.concat(F.lit('******'),F.substring(F.col("credit_card"),7,4)))
parquet_mask.show()

# Storing df in a single parquet file
parquet_mask.repartition(1).write.parquet("dataset/cast_mask_parquet")
----------------------------------------------------------------------------------
# Read MySQL table 

frame = pd.read_sql("select * from df_table limit 2", engine);
print(frame)
-----------------------------------------------------------------------------------
mydb = mysql.connector.connect(
  host="localhost",
  user="swapnil",
  password="swapnil123",
  database="mysql"
)
cursor = mydb.cursor()

query = "SELECT * FROM customers limit 10"   

cursor.execute(query)
for row in cursor:
    print(row)
#result = cursor.fetchall();
#result = cursor.fetchmany(2);
#result = cursor.fetchone();
#print(result)

cursor.close()
mydb.close()
-------------------------------------------------------------------------------

As I already worked on pyspark and SQL earlier so I didn't find this validations that much difficult, just some setup-part took lot of time, mainly python - mysql db connection, as there was some driver issue.
but yeah I did research and get it done 
But the with the coding part I'm already familier


