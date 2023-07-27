import pandas as pd
import json
import boto3
from datetime import datetime
import pytz
from sqlalchemy import create_engine
from io import StringIO
import requests

INDIATz = pytz.timezone("Asia/Kolkata") 
timeInINDIA = datetime.now(INDIATz)
currentTimeInINDIA = timeInINDIA.strftime("%Y%m%d%H%M%S")

# Open the json file and store all the inputs into variables
with open ("inputs_from_user.json", 'r', encoding = 'utf-8') as f:
    usr_inputs = json.load(f)
    model_URL = usr_inputs["model_link"]
    user_name = usr_inputs["Username"]
    project_name = usr_inputs["Project_Name"]
    data_source_type = usr_inputs["type_of_source"] 

# making the predictions
def make_predictions(dataframe):
    final_df = dataframe
    print(final_df.columns)
    model = pd.read_pickle(model_URL)
    print(model.feature_names_in_)
    prediction_vals = model.predict(final_df)
    final_df["Predictions"] = prediction_vals
    return  final_df

def dataurl():
    dataurl_cred = data_source_type["DataURL"]
    data_url = dataurl_cred["dataurl"]
    destination = dataurl_cred['destination_type']
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
        s3_object_name = f'{project_name}/{user_name}/{currentTimeInINDIA}_{project_name}_cleaned_dataset.csv'
        # Creating S3 Resource From the Session to store the data.
        s3_res = session.resource('s3')
        s3_res.Object(s3_bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())
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

print(dataurl())

