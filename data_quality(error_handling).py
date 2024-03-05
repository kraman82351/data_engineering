from pyspark import SparkContext, SQLContext
import pyspark
from pyspark.sql.functions import col, regexp_replace, when
import pandas
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
# q1_df = sqlC.sql("SELECT * FROM users WHERE fullName REGEXP '[0-9]'")
# policy_df = sqlC.sql("SELECT * FROM policies")
# # q3_df = sqlC.sql("SELECT C.* FROM claims AS C LEFT JOIN policies AS P using(insuranceId) WHERE C.claimedAmount > P.residualAmount")
# availablePolicies_df = sqlC.sql("SELECT * FROM availablePolicies")
q3_df = sqlC.sql("SELECT * FROM claims")
pandas_df = q3_df.toPandas()
# Iterate over each column in the DataFrame
# for col_name, col_type in policy_df.dtypes:

#     if col_type.startswith("int") or col_type.startswith("float"):

#         policy_df = policy_df.withColumn(col_name, when(col(col_name) == "", 0).otherwise(col(col_name)))
#     else:
        
#         policy_df = policy_df.withColumn(col_name, when(col(col_name) == "", "NA").otherwise(col(col_name)))

# availablePolicies_df = availablePolicies_df.withColumn("policyType", regexp_replace(col("policyType"), "\\s+", "_"))

# print("user Details if any user,s full Name contain any digits")
# q1_df.show()

# print("Policy data after eliminating NULL value")
# policy_df.show()

# print("claims Details whose claimed amount is more than residual amount")
# q3_df.show()

# print("Modified availablePolicies DataFrame:")
# availablePolicies_df.show()


# # writing the modified data frames into mongoDB
# policy_df.write.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_policies_ip).mode("overwrite").save()

# availablePolicies_df.write.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_availablePolices_ip).mode("overwrite").save()


pandas_df.to_csv("C:/Users/Aman/Desktop/claim management system/data_engineering/quality_check_datasheet/all_claims.csv", index=False)