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

# Construct MongoDB URI
mongo_users_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.users?retryWrites=true&w=majority"
mongo_policies_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.policies?retryWrites=true&w=majority"
mongo_claims_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.claims?retryWrites=true&w=majority"
mongo_availablePolices_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.availablepolicies?retryWrites=true&w=majority"

# Load data from MongoDB with the specified collection name
users = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_users_ip).load()
policies = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_policies_ip).load()
claims = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_claims_ip).load()
availablePolicies = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_availablePolices_ip).load()

# Create a temporary view
users.createOrReplaceTempView("users")
policies.createOrReplaceTempView("policies")
claims.createOrReplaceTempView("claims")
availablePolicies.createOrReplaceTempView("availablepolicies")

#creating data frames using queries
q1_df = sqlC.sql("SELECT * FROM users WHERE fullName REGEXP '[0-9]'")
q2_df = sqlC.sql("SELECT * FROM policies WHERE residualAmount < 0")
q3_df = sqlC.sql("SELECT C.* FROM claims AS C LEFT JOIN policies AS P using(insuranceId) WHERE C.claimedAmount > P.residualAmount")
q4_df = sqlC.sql("SELECT * FROM availablePolicies WHERE premium > coverageAmount")
columns_to_check = [col for col in policies.columns if col != '_id']
q5_df = sqlC.sql("SELECT * FROM policies WHERE " + " OR ".join(f"{col} IS NULL OR {col} = ''" for col in columns_to_check))


print("user Details if any user,s full Name contain any digits")
q1_df.show()

print("Policy data after eliminating NULL value")
q2_df.show()

print("claims Details whose claimed amount is more than residual amount")
q3_df.show()

print("Modified availablePolicies DataFrame:")
q4_df.show()

print("Policy with empty or null values in any field:")
q5_df.show()

pandas_q1_df = q1_df.toPandas()
pandas_q2_df = q2_df.toPandas()
pandas_q3_df = q3_df.toPandas()
pandas_q4_df = q4_df.toPandas()
pandas_q5_df = q5_df.toPandas()

pandas_q1_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/users_check.csv", index=False)
pandas_q2_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/policies_check.csv", index=False)
pandas_q3_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/claims_check.csv", index=False)
pandas_q4_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/availablePolicies_check.csv", index=False)
pandas_q5_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/policies_null_check.csv", index=False)