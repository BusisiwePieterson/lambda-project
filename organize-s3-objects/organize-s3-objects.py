import boto3
from datetime import datetime

today = datetime.today()
todays_date = today.strftime("%Y%m%d")

def lambda_handler(event, context):

    s3client = boto3.client('s3')

    bucket_name = "busi-organise-s3-object"
    list_objects_response = s3client.list_objects_v2(Bucket=bucket_name)


    get_contents = list_objects_response.get("Contents")
    get_all_s3_objects_and_folder_names = []

    for item in get_contents:
        s3_object_name = item.get("Key")
        get_all_s3_objects_and_folder_names.append(s3_object_name)
    get_all_s3_objects_and_folder_names


    directory_name = todays_date + "/"

    if directory_name not in get_all_s3_objects_and_folder_names:
        s3client.put_object(Bucket=bucket_name, Key=(directory_name))

    for item in get_contents:
        object_creation_date = item.get("LastModified").strftime("%Y%m%d") + "/"
        object_name = item.get("Key")

        if object_creation_date == directory_name and "/" not in object_name:
            s3client.copy_object(Bucket=bucket_name, CopySource=bucket_name+"/"+object_name, Key=directory_name+object_name)
            s3client.delete_object(Bucket=bucket_name, Key=object_name)






