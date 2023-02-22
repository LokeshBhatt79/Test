import json
import uuid
import os
import shutil
import json
import logging
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime, timedelta
import os.path
from boto3.dynamodb.conditions import Key, Attr
import pydicom
from pydicom.errors import InvalidDicomError
import pathlib
import urllib.parse


StatusFailedContactSupport = 7

#  - Checks the file is a dicom file                         #
##############################################################


def is_dicom(file: str) -> bool:
    isDicom = False
    try:
        print(f'inside is_dicom {file}')
        dataset = pydicom.read_file(file)
        isDicom = True
    except InvalidDicomError:
        # might be missing P10 header, which while invalid, we can recover
        print(
            "File - {file} - DICM prefix is missing from the header. Opening with force=True"
        )
        try:
            dataset = pydicom.read_file(file, force=True)
            # force will always parse, so now check to see if we have a valid DICOM structure
            # SOP Instance UID (0008, 0018) is a mandatory tag, if not the file is not useable as DICOM
            isDicom = (0x0008, 0x0018) in dataset
        except Exception as e:
            print("File - {file} - is not a valid dicom file. Marking this as a bad file.")
    #Checking if it is not a DICOMDIR file
    isDicomDir = True
    try:
        fs = FileSet(file)
        print("File - {file} - is a DICOMDIR file. Marking this as a bad file.")
    except:
        isDicomDir = False
    print(f"isDicom--{isDicom}")
    return isDicom and not(isDicomDir)


#####################################################################
#  - Gets the allowed extensions from Portal Db - tblPortalSettings #
#  - Pre-prerequisite
#         1. Environment variables - anonymizerRequiredQueue,nonprojectfileQueueurl,projectfileQueueurl	insights_projectfile_dev,tblPortalSettings
#         2. VPC
#         3. File System
#####################################################################
def get_Allowed_Extensions():

    # Getting Values from Enviroment Variables
    tblPortalSettings = os.environ["tblPortalSettings"]
    print(f"tbl---{tblPortalSettings}")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(tblPortalSettings)
    scan_kwargs1 = {
        "TableName": tblPortalSettings,
        "FilterExpression": Key("id").eq("6f64a80a-052f-4741-9e70-9ca018406cbe"),
        "ProjectionExpression": "dicomExtensions,projectFileExtensions",
        
    }
    print(f"data1---{scan_kwargs1}")
    data1 = table.scan(**scan_kwargs1)["Items"]
    print(f"data1---{data1}")
    if " " in data1[0]["dicomExtensions"]:
        data1[0]["dicomExtensions"].append("")
    return data1[0]


#######################################################################################################################################
#  - Moves the Non Project Files the BadFiles Folder in S3                                                                                                       #
#######################################################################################################################################



def handler(event, context):

    # intilization
    s3 = boto3.resource("s3", region_name="us-east-2")
    sqs_client = boto3.client("sqs")
    dbQueueUrl = os.environ["dbqueueurl"]
    logActivityQueueUrl = os.environ["logactivityqueueurl"]
    
    dicomFiles = 0

    # Getting the SQS object url
    sqs_queue_url_housekeeping = sqs_client.get_queue_url(QueueName=dbQueueUrl)[
        "QueueUrl"
    ]
    sqs_queue_url_logactivity = sqs_client.get_queue_url(QueueName=logActivityQueueUrl)[
        "QueueUrl"
    ]

    for items in event["Records"]:
        eventSource = json.loads(items["body"])
        key = eventSource["fullPath"].split("/public/")
        sourceKey = "public/" + key[1]
        
        try:
            fileName = os.path.basename(sourceKey)
            startTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            record = {}
            record["RecordUpdate"] = "Update"
            record["logCount"] = str(datetime.now().strftime("%M:%S.%fZ"))
            record["type"] = "apkFile"
            record["stage"] = "Apk File Movement"
            record["startTime"] = startTime
            record["Records"] = eventSource["fullPath"]
            record["sourceKey"] = sourceKey
            record["scanId"] = eventSource["scanId"]

            # Getting Values from Enviroment Variables
            outputBucketName = os.environ["outputBucket"]

            outputBucket = s3.Bucket(outputBucketName)

            visitSplit = sourceKey.split("/")
            print(f'sourceKey{sourceKey}')
            print(f'sourceKey{sourceKey.split(visitSplit[5])[1]}')
            print(f'sourceKey{sourceKey.split(visitSplit[5])[0]}')
            s3auxilaryKey = (
                sourceKey.split(visitSplit[5])[0]
                + visitSplit[5]
                + "/Apk"
                + sourceKey.split(visitSplit[6])[1]
            )
            print(f'file_extension--')

            file_extension = pathlib.Path(sourceKey).suffix
            print(f'file_extension--{file_extension}')

            allowedExtensions = get_Allowed_Extensions()
            print(f"allowedExtensions--{allowedExtensions}")
            print(f"sourceKey--{sourceKey}")
            print(f"typeof----{type(eventSource['scanId'])}")
            if file_extension.lower() in allowedExtensions[
                        "dicomExtensions"
                    ] and is_dicom( "/mnt/efs/" + eventSource["scanId"] +"/" +sourceKey):
                        record["type"] = "apk_dicom"
                        print(f'dicom condition {dicomFiles}')
                        
            ########################################################################
            #            Replacing file in EFS with updated dicom file             #
            ########################################################################
            Visit_path = "/".join(sourceKey.split("/")[:6])
            EFS_STORAGE_PATH = (
                "/mnt/efs/" + eventSource["scanId"] + "/" + Visit_path + "/"
            )
            efs_auxilaryFolder = EFS_STORAGE_PATH + "Apk"
            efs_auxilaryFilePath = efs_auxilaryFolder+sourceKey.split(visitSplit[5])[1]
            os.makedirs(efs_auxilaryFilePath.replace(fileName,""), exist_ok=True)
            shutil.copy(eventSource["fullPath"], efs_auxilaryFilePath)
            ########################################################################
            #            Replaced file in EFS with updated dicom file              #
            ########################################################################
            print(f"auxilarykey--{s3auxilaryKey}")
            print(f"fullPath--{eventSource['fullPath']}")

            outputBucket.upload_file(eventSource["fullPath"], s3auxilaryKey)
            os.remove(eventSource["fullPath"])
            endTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            record["status"] = "moved"
            record["message"] = f"{fileName} - apk file moved Successfully"
            record["endTime"] = endTime

            sqs_client.send_message(
                QueueUrl=sqs_queue_url_housekeeping, MessageBody=json.dumps(record)
            )  # Updates the  file status to processed
            sqs_client.send_message(
                QueueUrl=sqs_queue_url_logactivity, MessageBody=json.dumps(record)
            )  # Logging activy

            print(f"{fileName} - Apk file moved Successfully")

        except Exception as e:
            endTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            record["status"] = StatusFailedContactSupport
            record[
                "message"
            ] = f"{fileName} - Failed moving Apk file with error {str(e)}"
            record["endTime"] = endTime

            sqs_client.send_message(
                QueueUrl=sqs_queue_url_housekeeping, MessageBody=json.dumps(record)
            )  # Updates the  file status to failed
            sqs_client.send_message(
                QueueUrl=sqs_queue_url_logactivity, MessageBody=json.dumps(record)
            )  # Logging activy

            print(f"{fileName} - Failed moving apk file with error {str(e)}")

    return {"statusCode": 200, "body": json.dumps("Success!")}
