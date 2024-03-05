from pyspark import SparkContext, SQLContext
import pyspark
from datetime import datetime, timedelta
import pytz

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

def get_timestamp():
    current_time = datetime.now(pytz.utc)
    
    previous_time = current_time - timedelta(hours=24)
    
    return previous_time.strftime("%Y-%m-%d %H:%M:%S")


# Define the provided timestamp
created_timestamp = get_timestamp()
updated_timestamp = get_timestamp()

# Query to select users whose createdAt timestamp is more recent than the provided timestamp
created_users = sqlC.sql(f"SELECT * FROM users WHERE createdAt > '{created_timestamp}'")
updated_users = sqlC.sql(f"SELECT * FROM users WHERE updatedAt > '{updated_timestamp}'")

# Display the user details
print(f"User details created in last 24hrs : '{created_timestamp}'")
created_users.show()

print(f"User details Updated in last 24hrs : '{updated_timestamp}'")
updated_users.show()
