import csv
from os import access
import subprocess

import requests
from click import command
from dagster import get_dagster_logger, job, op
from minio import Minio
from minio.error import S3Error

minio_url = ""
minio_accessk = ""
minio_secretk = ""
minio_bucket_name = ""

video_url = "rtsp://<user>:<password>@<IP>/h264/2"
video_output = "ffmpeg_capture.mp4"

@op
def download_video(video_url):
    command =["ffmpeg", "-rtsp_transport", "tcp", "-i", video_url, "-vcodec",
        "copy", "-map", "0", "-f", "segment", "-segment_time", "300", "-segment_format", 
        "mp4", video_output]
    output,error  = subprocess.Popen(
                        command, universal_newlines=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output

@op
def upload_minio(download_video):
    client = Minio(
        minio_url,
        access_key=minio_accessk,
        secret_key=minio_secretk,
    )

    found = client.bucket_exists(minio_bucket_name)
    if not found:
        client.make_bucket(minio_bucket_name)
    else:
        print("Bucket {} already exists".format(minio_bucket_name))

    client.fput_object(
        minio_bucket_name, video_output, video_output,
    )

@job
def download_and_upload():
    output = download_video()
    upload_minio(output)

