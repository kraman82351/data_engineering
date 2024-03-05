from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import XCom


import pymongo

# get data according to the collection name
def get_data(mongo_collection):
    mongo_username = "kraman82351"
    mongo_password = "9GSe4uDJAJFRSX1L"
    mongo_cluster = "cluster0"
    mongo_database = "test"

    mongo_ip = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}.0lanvvt.mongodb.net/{mongo_database}.{mongo_collection}?retryWrites=true&w=majority"

    client = pymongo.MongoClient(mongo_ip)

    db = client[mongo_database]

    collection = db[mongo_collection]

    data = collection.find()
  
    data_list = [doc for doc in data]

    for doc in data_list:
        doc['_id'] = str(doc['_id'])
        
    return data_list




def check_users_data():
    users_data = get_data("users")    
    # Check each user's fullName attribute
    for user in users_data:
        print(user)
        



def check_policies_data():
    policies_data = get_data("policies")
    for policy in policies_data:
        print(policy)
        


def check_claims_data():
    claims_data = get_data("claims")
    for claim in claims_data:
        print(claim)


dag = DAG(
    'Sequential_Tasks',
    schedule_interval=None,
    start_date=datetime(2024, 2, 1),
    catchup=False
)


with dag:
    check_users = PythonOperator(
        task_id='check_users',
        python_callable=check_users_data,

    )


    check_policies = PythonOperator(
        task_id='check_policies',
        python_callable=check_policies_data,
    )

    check_claims = PythonOperator(
        task_id='check_claims',
        python_callable=check_claims_data,
    )



    check_users >> check_policies >> check_claims