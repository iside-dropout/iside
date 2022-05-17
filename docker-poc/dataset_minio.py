import tempfile
import zipfile

from minio import Minio


def get_from_s3(host, access_key, secret_key, bucket, local_dir, secure_connection=True, objname="full_dataset.zip"):
    """Retrieve the dataset from S3

    :param access_key: S3 Access key
    :type access_key: string
    :param secret_key: S3 Secret key
    :type secret_key: string
    :param bucket: S3 bucket name
    :type bucket: string
    :param local_dir: Local directory where the dataset will be saved
    :type local_dir: string
    """
    client = Minio(
        endpoint=host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure_connection,
    )

    found = client.bucket_exists(bucket)
    if not found:
        raise Exception("Bucket not found in S3")

    tmpdir = tempfile.TemporaryDirectory()
    client.fget_object(bucket, objname, tmpdir.name + "/dataset.zip")

    with zipfile.ZipFile(tmpdir.name + "/dataset.zip", 'r') as zip_ref:
        zip_ref.extractall(local_dir)


def put_into_s3(host, access_key, secret_key, bucket, objname, contentstream, contentlength, secure_connection=True):
    """Save object into S3

    :param access_key: S3 Access key
    :type access_key: string
    :param secret_key: S3 Secret key
    :type secret_key: string
    :param bucket: S3 bucket name
    :type bucket: string
    """
    client = Minio(
        endpoint=host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure_connection,
    )

    found = client.bucket_exists(bucket)
    if not found:
        raise Exception("Bucket not found in S3")

    client.put_object(bucket, objname, contentstream, contentlength)
