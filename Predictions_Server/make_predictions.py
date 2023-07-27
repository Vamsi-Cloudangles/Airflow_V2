import pandas as pd
import json
from io import StringIO
import requests
import boto3
from datetime import datetime
import pytz
import pymysql
import psycopg2

INDIATz = pytz.timezone("Asia/Kolkata") 
timeInINDIA = datetime.now(INDIATz)
currentTimeInINDIA = timeInINDIA.strftime("%Y%m%d-%H%M%S")

# Open the json file and store all the inputs into variables
with open ("inputs_from_user.json", 'r', encoding = 'utf-8') as f:
    usr_inputs = json.load(f)
    model_URL = usr_inputs["model_link"]
    user_name = usr_inputs["Username"]
    project_name = usr_inputs["Project_Name"]
    bucket_name = usr_inputs["mlangles_s3_bucketname"]
    data_source_type = usr_inputs["type_of_source"]
    type_of_source = list(data_source_type.keys())
              
# Connect to the mysql_database and get the user's cleaned dataframe
def mysql_database():
    mysql_inputs = usr_inputs["type_of_source"]["mysql_db"]
    MySQL_db_Hostname = mysql_inputs["mysql_hostname"]
    MySQL_db_Username = mysql_inputs["mysql_username"]
    MySQL_db_Password = mysql_inputs["mysql_password"]
    MySQL_db_Databsename = mysql_inputs["mysql_databasename"]
    MySQL_db_Tablename = mysql_inputs["mysql_database_tablename"]
    mysql_db_connect = pymysql.connect( host = MySQL_db_Hostname, 
                                        user = MySQL_db_Username, 
                                        password = MySQL_db_Password, 
                                        database = MySQL_db_Databsename )
    sql_query = f"SELECT * FROM {MySQL_db_Tablename}"
    mysql_db_data = pd.read_sql(sql_query, mysql_db_connect)
    return mysql_db_data

# Connect to the POSTGRES SQL database and get the user's cleaned dataframe
def postgres_database():
    postgres_inputs = usr_inputs["type_of_source"]["postgres_db"]
    PostGreSQL_db_Hostname = postgres_inputs["postgres_db_hostname"]
    PostGreSQL_db_Username = postgres_inputs["postgres_db_username"]
    PostGreSQL_db_Password = postgres_inputs["postgres_db_password"]
    PostGreSQL_db_Databsename = postgres_inputs["postgres_db_databasename"]
    # PostGreSQL_db_Schemaname = postgres_inputs["postgres_db_schemaname"]
    PostGreSQL_db_Tablename = postgres_inputs["postgres_db_tablename"]

    postgres_db_connect = psycopg2.connect( host = PostGreSQL_db_Hostname,
                                            user = PostGreSQL_db_Username,
                                            password = PostGreSQL_db_Password,
                                            database = PostGreSQL_db_Databsename )
    postgres_query = f"SELECT * FROM {PostGreSQL_db_Tablename};"
    postgres_db_data = pd.read_sql(postgres_query, postgres_db_connect)
    return postgres_db_data

# Make the predictions and stores in a dataframe  
def make_predictions():
    if (type_of_source[0] == 'DataURL'):
        data_url = usr_inputs["type_of_source"]["DataURL"]["dataurl"]
        response = requests.get(f'{data_url}', verify = False)
        data = response.text
        final_df = pd.read_csv(StringIO(data))
    elif (type_of_source[0] == 'mysql_db'):
        final_df = mysql_database()
    elif (type_of_source[0] == 'postgres_db'):
        final_df = postgres_database()

    print(final_df.columns)
    model = pd.read_pickle(model_URL)
    print(model.feature_names_in_)
    prediction_vals = model.predict(final_df)
    final_df["Predictions"] = prediction_vals
    return final_df

# Store all the predictions with input file in the S3 bucket 
def store_in_s3():
    session = boto3.Session( aws_access_key_id='AKIA3YG72WSKDDWFQLEM',
                             aws_secret_access_key='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'
                            )
    #Creating S3 Resource From the Session.
    s3_res = session.resource('s3')
    csv_buffer = StringIO()
    final_df = make_predictions()
    final_df.to_csv(csv_buffer, index = False)
    s3_object_name = f'{project_name}/{user_name}/{project_name}_{currentTimeInINDIA}_cleaned_dataset.csv'
    s3_res.Object(bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())

store_in_s3()

