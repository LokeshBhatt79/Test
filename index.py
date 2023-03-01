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
        except Exception:
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
        except Exception:
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
