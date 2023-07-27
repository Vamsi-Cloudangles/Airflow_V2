from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import time
import json
import paramiko

ami = "ami-0fcbde48d6b768eca"
with open ("/opt/airflow/dags/airflow_inputs.json", "r", encoding = 'utf-8' ) as f:
    usr_inputs = json.load(f)
    instance_type_lst = usr_inputs["Instance_Type"]
    dag_ids_lst = usr_inputs["Dags_Id"]
    usernames_lst = usr_inputs["Username"]
    schedule_times_lst = usr_inputs["Schedule_time"]
    pr_names_lst = usr_inputs["Project_Name"]
    model_links_lst = usr_inputs["model_link"]
    type_of_sources_lst = usr_inputs["type_of_source"]

def create_server_py(**kwargs):
    prediction_inputs = kwargs["inputs"]
    instance_type = prediction_inputs["Type_Of_Instance"]
    print(prediction_inputs)
# Create an Ec2 instance based on the requirements
    ec2 = boto3.client('ec2', 
                        'us-east-1', 
                        aws_access_key_id='AKIA3YG72WSKCSQZ3X37',
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
            # "cd Batch_Predictions && python3 make_predictions.py"
            "whoami"
            ]
    for c in cmds:
        stdin, stdout, stderr = ssh_client.exec_command(c)
        print(stdout.read())
        print(stderr.read())

    # ec2.terminate_instances(InstanceIds=[Instance_ID])

def prediction_inputs(u_name, pr_name, mdl_link,source_data, instance_type):
    user_inputs = {
                    "Type_Of_Instance": instance_type,
                    "Username": u_name ,
                    "Project_Name": pr_name,
                    "model_link": mdl_link,
                    "type_of_source": source_data,
                    }
    return user_inputs

for k in range(len(dag_ids_lst)):
    type_of_source = list(type_of_sources_lst[k].keys())
    if (type_of_source[0] == 'DataURL'):
        args = {"owner": usernames_lst[k], "start_date": days_ago(1)}
        pred_inputs = prediction_inputs(usernames_lst[k], pr_names_lst[k], model_links_lst[k], type_of_sources_lst[k], instance_type_lst[k])
        dag = DAG(
                    dag_id = dag_ids_lst[k],
                    default_args = args,
                    schedule_interval = schedule_times_lst[k])
        
    elif (type_of_source[0] == 's3'):
        args = {"owner": usernames_lst[k], "start_date":days_ago(1)}
        pred_inputs = prediction_inputs(usernames_lst[k], pr_names_lst[k], model_links_lst[k], type_of_sources_lst[k], instance_type_lst[k])
        dag = DAG(
                    dag_id = dag_ids_lst[k],
                    default_args = args,
                    schedule_interval = schedule_times_lst[k])
        
    elif (type_of_source[0] == 'mysql_db'):
        args = {"owner": usernames_lst[k], "start_date":days_ago(1)}
        pred_inputs = prediction_inputs(usernames_lst[k], pr_names_lst[k], model_links_lst[k], type_of_sources_lst[k], instance_type_lst[k])
        dag = DAG(
                    dag_id = dag_ids_lst[k],
                    default_args = args,
                    schedule_interval = schedule_times_lst[k])
        
    elif (type_of_source[0] == 'postgres_db'):
        args = {"owner": usernames_lst[k], "start_date":days_ago(1)}
        pred_inputs = prediction_inputs(usernames_lst[k], pr_names_lst[k], model_links_lst[k], type_of_sources_lst[k], instance_type_lst[k])
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
        
    