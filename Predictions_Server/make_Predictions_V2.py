import pandas as pd
import json
from io import StringIO
import requests
import boto3
from datetime import datetime
import pytz
from sqlalchemy import create_engine

INDIATz = pytz.timezone("Asia/Kolkata") 
timeInINDIA = datetime.now(INDIATz)
currentTimeInINDIA = timeInINDIA.strftime("%Y%m%d%H%M%S")

# Open the json file and store all the inputs into variables
with open ("inputs_from_user2.json", 'r', encoding = 'utf-8') as f:
    usr_inputs = json.load(f)
    model_URL = usr_inputs["model_link"]
    user_name = usr_inputs["Username"]
    project_name = usr_inputs["Project_Name"]
    data_source_type = usr_inputs["type_of_source"]
    type_of_source = list(data_source_type.keys())

# making the predictions
def make_predictions(dataframe):
    final_df = dataframe
    print(final_df.columns)
    model = pd.read_pickle(model_URL)
    print(model.feature_names_in_)
    prediction_vals = model.predict(final_df)
    final_df["Predictions"] = prediction_vals
    return  final_df
       
# Connect to the mysql_database and get the user's cleaned dataframe
def mysql_database():
    mysql_inputs = usr_inputs["type_of_source"]["mysql_db"]
    MySQL_db_Hostname = mysql_inputs["mysql_hostname"]
    MySQL_db_Username = mysql_inputs["mysql_username"]
    MySQL_db_Password = mysql_inputs["mysql_password"]
    MySQL_db_Databsename = mysql_inputs["mysql_databasename"]
    MySQL_db_Tablename = mysql_inputs["mysql_database_tablename"]
    # Connect to mysql database and create a table then store the dataframe with predictions
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host = MySQL_db_Hostname, db = MySQL_db_Databsename, user = MySQL_db_Username, pw = MySQL_db_Password))
    sql_query = f"SELECT * FROM {MySQL_db_Tablename}"
    mysql_db_data = pd.read_sql(sql_query, engine)
    dataframe_with_predictions = make_predictions(mysql_db_data)
    table_name = f"{project_name}{currentTimeInINDIA}withpredictions"
    dataframe_with_predictions.to_sql(f'{table_name}', engine, index=False)
    return ("Successfully stored in mysql_database")

# Connect to the POSTGRES SQL database and get the user's cleaned datafile
def postgres_database():
    postgres_inputs = usr_inputs["type_of_source"]["postgres_db"]
    PostGreSQL_db_Hostname = postgres_inputs["postgres_db_hostname"]
    PostGreSQL_db_Username = postgres_inputs["postgres_db_username"]
    PostGreSQL_db_Password = postgres_inputs["postgres_db_password"]
    PostGreSQL_db_Databsename = postgres_inputs["postgres_db_databasename"]
    # PostGreSQL_db_Schemaname = postgres_inputs["postgres_db_schemaname"]
    PostGreSQL_db_Tablename = postgres_inputs["postgres_db_tablename"]

    engine = create_engine("postgresql://{user}:{pw}@{host}/{db}"
				.format(host = PostGreSQL_db_Hostname, db = PostGreSQL_db_Databsename, user = PostGreSQL_db_Username, pw = PostGreSQL_db_Password))
    postgres_query = f"SELECT * FROM {PostGreSQL_db_Tablename};"
    postgres_db_data = pd.read_sql(postgres_query, engine)
    dataframe_with_predictions = make_predictions(postgres_db_data)
    table_name = f"{project_name}{currentTimeInINDIA}withpredictions"
    dataframe_with_predictions.to_sql(f'{table_name}', engine, index=False)
    return ("Successfully stored in Postgresql_database")

# Connect to the S3 bucket and get the user's cleaned datafile
def s3_bucket():
    s3_inputs = usr_inputs["type_of_source"]["s3"]
    s3_access_key_id = s3_inputs["AWS_ACCESS_KEY_ID"]
    s3_secret_access_key = s3_inputs["AWS_SECRET_ACCESS_KEY"]
    s3_bucket_name = s3_inputs["Bucket_Name"]
    object_path = s3_inputs["Object_Path"]

    session = boto3.Session(
                            aws_access_key_id = s3_access_key_id,
                            aws_secret_access_key = s3_secret_access_key
                            )
    # creating s3 client for the data read
    s3_cli = session.client('s3')
    response = s3_cli.get_object(Bucket = s3_bucket_name, Key = object_path)
    s3_bucket_data = pd.read_csv(response.get("Body")) 
    dataframe_with_predictions = make_predictions(s3_bucket_data) 
    csv_buffer = StringIO()
    dataframe_with_predictions.to_csv(csv_buffer, index = False)
    s3_object_name = f'mlangles{project_name}/{user_name}/{currentTimeInINDIA}_{project_name}_cleaned_dataset.csv'
    # Creating S3 Resource From the Session to store the data.
    s3_res = session.resource('s3')
    s3_res.Object(s3_bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())
    return ("Successfully stored in s3_bucket")

def dataurl():
    dataurl_cred = data_source_type["DataURL"]
    print(data_source_type)
    data_url = dataurl_cred["dataurl"]
    destination = dataurl_cred['destination_type']
    print(destination)
    response = requests.get(f'{data_url}', verify = False)
    data = response.text
    dataframe = pd.read_csv(StringIO(data))
    dataframe_with_predictions = make_predictions(dataframe)

    # stores the predictions data in s3
    if (destination == 's3'):
        s3_access_key_id = dataurl_cred["AWS_ACCESS_KEY_ID"]
        s3_secret_access_key = dataurl_cred["AWS_SECRET_ACCESS_KEY"]
        s3_bucket_name = dataurl_cred["Bucket_Name"]
        csv_buffer = StringIO()
        dataframe_with_predictions.to_csv(csv_buffer, index = False)
        session = boto3.Session(
                            aws_access_key_id = s3_access_key_id,
                            aws_secret_access_key = s3_secret_access_key
                            )
        s3_object_name = f'mlangles/{project_name}/{user_name}/{currentTimeInINDIA}_{project_name}_cleaned_dataset.csv'
        # Creating S3 Resource From the Session to store the data.
        s3_res = session.resource('s3')
        s3_res.Object(s3_bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())
        return (f"data stored successfully in {destination}.")
    # stores the predictions data in postgresql_db
    elif (destination == 'postgres_db'):
        PostGreSQL_db_Hostname = dataurl_cred["postgres_db_hostname"]
        PostGreSQL_db_Username = dataurl_cred["postgres_db_username"]
        PostGreSQL_db_Password = dataurl_cred["postgres_db_password"]
        PostGreSQL_db_Databsename = dataurl_cred["postgres_db_databasename"]
        engine = create_engine("postgresql://{user}:{pw}@{host}/{db}"
				.format(host = PostGreSQL_db_Hostname, db = PostGreSQL_db_Databsename, user = PostGreSQL_db_Username, pw = PostGreSQL_db_Password))
        table_name = f"{project_name}{currentTimeInINDIA}withpredictions"
        dataframe_with_predictions.to_sql(f'{table_name}', engine, index=False)
        return (f"data stored successfully in {destination}.")
    # stores the predictions data in mysql_db
    elif (destination == 'mysql_db'):
        MySQL_db_Hostname = dataurl_cred["postgres_db_hostname"]
        MySQL_db_Username = dataurl_cred["postgres_db_username"]
        MySQL_db_Password = dataurl_cred["postgres_db_password"]
        MySQL_db_Databsename = dataurl_cred["postgres_db_databasename"]
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host = MySQL_db_Hostname, db = MySQL_db_Databsename, user = MySQL_db_Username, pw = MySQL_db_Password))
        table_name = f"{project_name}{currentTimeInINDIA}withpredictions"
        dataframe_with_predictions.to_sql(f'{table_name}', engine, index=False)
        return (f"data stored successfully in {destination}.")

# Make the predictions and stores in a dataframe  
def select_datasource():
    print(type_of_source[0])
    if (type_of_source[0] == 'DataURL'):
        dataurl()
    elif (type_of_source[0] == 'mysql_db'):
         mysql_database()
    elif (type_of_source[0] == 'postgres_db'):
        postgres_database()
    elif (type_of_source[0] == 's3'):
        s3_bucket()

print(select_datasource())