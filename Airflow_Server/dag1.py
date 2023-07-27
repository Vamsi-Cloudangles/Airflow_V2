from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import time
import json
import paramiko

ami = "ami-0bdaf83af3345f410"
start_dates_lst = [days_ago(1), days_ago(1)]
with open ("/opt/airflow/dags/airflow_inputs.json", "r", encoding = 'utf-8' ) as f:
    usr_inputs = json.load(f)
    instance_type_lst = usr_inputs["Instance_Type"]
    dag_ids_lst = usr_inputs["Dags_Id"]
    usernames_lst = usr_inputs["Username"]
    schedule_times_lst = usr_inputs["Schedule_time"]
    pr_names_lst = usr_inputs["Project_Name"]
    model_links_lst = usr_inputs["model_link"]
    mlangles_s3_buckets_name_lst = usr_inputs["mlangles_s3_bucketname"]
    type_of_sources_lst = usr_inputs["type_of_source"]
    data_URLs_lst = usr_inputs["DataURL"]
    databases_lst = usr_inputs["data_base"]

def create_server_py(**kwargs):
    prediction_inputs = kwargs["inputs"]
    instance_type = prediction_inputs["Type_Of_Instance"]
    print(prediction_inputs)
# Create an Ec2 instance based on the requirements
    ec2 = boto3.client('ec2', 
                        'us-east-1', 
                        aws_access_key_id='AKIA3YG72WSKDDWFQLEM',
                        aws_secret_access_key='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'
                     )
    create_ec2 = ec2.run_instances(InstanceType = instance_type,  
                                    MaxCount = 1, 
                                    MinCount = 1, 
                                    ImageId = ami,
                                    KeyName = "test"
                                  )
    # getting the instance id
    instance_id = create_ec2['Instances'][0]['InstanceId']
    Instance_ID = str(instance_id)
    print(Instance_ID)
    time.sleep(90)
    # Getting the instance IP from instnce id
    reservations = ec2.describe_instances(InstanceIds=[Instance_ID]).get("Reservations")
    for reservation in reservations:
        for instance in reservation['Instances']:
            public_ip_address = instance.get("PublicIpAddress")
    Predictions_Machine_public_ip = str(public_ip_address)

    # Connect to predictions machine via ssh and run the predictions python file to get the predictions.
    # Connect to ssh
    ssh_client=paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key = paramiko.RSAKey.from_private_key_file('/opt/airflow/dags/test.pem')
    ssh_client.connect(hostname = Predictions_Machine_public_ip, username='ubuntu', pkey = key)
    ftp_client=ssh_client.open_sftp()
    v = ftp_client.open('Batch_Predictions/inputs_from_user.json', "wb")
    json.dump(prediction_inputs, v)
    cmds = [
            "cd Batch_Predictions && python3 make_predictions.py"
            ]
    for c in cmds:
        stdin, stdout, stderr = ssh_client.exec_command(c)
        print(stdout.read())
        print(stderr.read())

    ec2.terminate_instances(InstanceIds=[Instance_ID])

for k in range(len(dag_ids_lst)):
    args = {"owner": usernames_lst[k], "start_date":start_dates_lst[k]}
    pred_inputs = {
                    "Type_Of_Instance": instance_type_lst[k],
                    "Username": usernames_lst[k],
                    "Project_Name": pr_names_lst[k],
                    "model_link": model_links_lst[k],
                    "mlangles_s3_bucketname": mlangles_s3_buckets_name_lst[k],
                    "type_of_source": type_of_sources_lst[k],
                    "Data_URL":data_URLs_lst[k],
                    "database":databases_lst[k]
                    }
    dag = DAG(
                dag_id = dag_ids_lst[k],
                default_args = args,
                schedule_interval = schedule_times_lst[k])
    with dag:
        hello_world = PythonOperator(
                                     task_id = 'batch_processing',
                                     python_callable = create_server_py,
                                     op_kwargs={"inputs":pred_inputs}
                                     )
        
    