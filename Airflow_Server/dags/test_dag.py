import boto3
import time
import json
import paramiko

ami = "ami-0fcbde48d6b768eca"
with open ("test.json", "r", encoding = 'utf-8' ) as f:
    usr_inputs = json.load(f)
    instance_type_lst = usr_inputs["Instance_Type"]
    dag_ids_lst = usr_inputs["Dags_Id"]
    usernames_lst = usr_inputs["Username"]
    schedule_times_lst = usr_inputs["Schedule_time"]
    pr_names_lst = usr_inputs["Project_Name"]
    model_links_lst = usr_inputs["model_link"]
    type_of_sources_lst = usr_inputs["type_of_source"]

def create_server_py():
    instance_type = "t2.xlarge"
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
    return Predictions_Machine_public_ip

    # # Connect to predictions machine via ssh and run the predictions python file to get the predictions.
    # # Connect to ssh
    # ssh_client=paramiko.SSHClient()
    # ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # key = paramiko.RSAKey.from_private_key_file('test.pem')
    # ssh_client.connect(hostname = Predictions_Machine_public_ip, username='ubuntu', pkey = key)
    # ftp_client=ssh_client.open_sftp()
    # v = ftp_client.open('Batch_Predictions/inputs_from_user.json', "wb")
    # json.dump(prediction_inputs, v)
    # cmds = [
    #         # "cd Batch_Predictions && python3 make_predictions.py"
    #         "whoami"
    #         ]
    # for c in cmds:
    #     stdin, stdout, stderr = ssh_client.exec_command(c)
    #     print(stdout.read())
    #     print(stderr.read())

    # ec2.terminate_instances(InstanceIds=[Instance_ID])

print(create_server_py())

