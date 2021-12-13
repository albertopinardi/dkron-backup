import json
import typer
from requests.exceptions import RequestException
import requests
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

app = typer.Typer()

dkronfolder = os.environ.get("HOME",".")+"/.dkron-backup"

def create_local_dir():
    try:
        os.makedirs(dkronfolder, exist_ok=True)
    except FileExistsError:
        pass
    try:
        os.makedirs(dkronfolder+"/backups", exist_ok=True)
    except FileExistsError:
        pass
    try:
        os.makedirs(dkronfolder+"/tmp", exist_ok=True)
    except FileExistsError:
        pass
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        typer.echo(e,err=True)
        return False
    typer.echo(response)
    return True

@app.command()
def backup(
        url: str = typer.Argument("http://localhost:8080", envvar="DKRON_URL"),
        s3_bucket: str = typer.Option(None, envvar="DKRON_S3_BUCKET")
    ):
    time = datetime.now()
    headers = {"accept": "application/json"}
    try:
        response = requests.get(
            url+"/v1/jobs",
            headers=headers
        )
    except RequestException as e:
        typer.echo(e,err=True)
        raise typer.Exit(code=1)
    create_local_dir()
    try:
        with open(dkronfolder+"/tmp/dkron-backup-latest.json","w") as f:
            json.dump(response.json(),f)
    except FileExistsError as e:
        typer.echo("There is already a backup in the default folder. Moving it out of the way")
    if s3_bucket:
        upload = upload_file(dkronfolder+"/tmp/dkron-backup-latest.json", s3_bucket,"dkron-backup_"+time.strftime("%y%m%d_%H_%M")+".json")
        typer.echo("Upload to S3 succesfull") if upload else typer.echo("Upload to S3 failed")
    try:
        os.rename(dkronfolder+"/tmp/dkron-backup-latest.json",dkronfolder+"/backups/dkron-backup_"+time.strftime("%y%m%d_%H_%M")+".json")
    except OSError as e:
        typer.echo(e,err=True)
        raise typer.Exit(code=1)
    except NotImplementedError as e:
        typer.echo(e,err=True)
        typer.echo("Moving files not supported on this OS")
        raise typer.Exit(code=1)


@app.command()
def restore(
        url: str = typer.Argument("http://localhost:8080", envvar="DKRON_URL"),
        source: str = typer.Argument(dkronfolder+"/tmp/dkron-backup-latest.json")
    ):
    header = {"accept": "application/json"}
    try:
        with open(source,"r") as f:
            restore = json.load(f)
    except FileNotFoundError as e:
        typer.echo(e,err=True)
    typer.echo(f"There are {restore.__len__()} jobs to restore")
    for job in restore:
        try:
            response = requests.post(
                url+"/v1/jobs",
                headers=header,
                json=job
            )
        except RequestException as e:
            typer.echo(e, err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{job.get('name')} {response.status_code} {response.reason}")


if __name__ == "__main__":
    app()
