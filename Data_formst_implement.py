from pyspark import SparkContext, SQLContext
import pyspark

# Set up Spark configuration
conf = pyspark.SparkConf().set("spark.jars.packages",
                               "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                           .setMaster("local") \
                           .setAppName("MyApp") \
                           .setAll([("spark.driver.memory", "40g"), ("spark.executor.memory", "50g")])

# Entry point to PySpark
sc = SparkContext(conf=conf)
sqlC = SQLContext(sc)

# Replace the placeholder values below with your actual MongoDB Atlas credentials and cluster details
mongo_username = "kraman82351"
mongo_password = "9GSe4uDJAJFRSX1L"
mongo_cluster = "cluster0"
mongo_database = "test"
mongo_collection = "users"  # Update to your collection name

# Construct MongoDB URI
mongo_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.{mongo_collection}?retryWrites=true&w=majority"
print(mongo_ip)

# Load data from MongoDB with the specified collection name
users = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip).load()

# Create a temporary view
users.createOrReplaceTempView("users")

# Define the provided timestamp
created_timestamp = "2024-02-21 12:00:00"

# Query to select users whose createdAt timestamp is more recent than the provided timestamp
created_users = sqlC.sql(f"SELECT fullName,emailId, createdAt, updatedAt FROM users WHERE createdAt > '{created_timestamp}'")

# Display the user details
print(f"User details created after timestamp : '{created_timestamp}'")
created_users.show()

parquet_path = "C:/Users/Aman/Desktop/claim management system/data_engineering/parquet/"
csv_path = "C:/Users/Aman/Desktop/claim management system/data_engineering/CSV/"

created_users.write.mode("append").parquet(parquet_path)

parquet_df = sqlC.read.parquet(parquet_path)

# Display schema and content of the Parquet dataframe
print("Schema of Parquet dataframe:")
parquet_df.printSchema()
print("Content of Parquet dataframe:")
parquet_df.show()

parquet_df.write.mode("append").option("header", "true").csv(csv_path)

csv_df = sqlC.read.option("header", "true").csv(csv_path)

# Display schema and content of the CSV dataframe
print("Schema of CSV dataframe:")
csv_df.printSchema()
print("Content of CSV dataframe:")
csv_df.show()
print("Data saved successfully.")