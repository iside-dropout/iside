import json
import logging
import os
import tempfile

import tink
from tink import cleartext_keyset_handle, aead

from dataset_minio import get_from_s3

aead_primitive = None
encrypted_dataset_dir = None
aead.register()


def init_encrypt():
    global aead_primitive
    global encrypted_dataset_dir
    if "KEYSET" not in os.environ:
        raise Exception("missing decryption configuration")

    keyset_reader = tink.JsonKeysetReader(open(os.environ["KEYSET"], "rt").read())
    keyset_handle = cleartext_keyset_handle.read(keyset_reader)
    aead_primitive = keyset_handle.primitive(aead.Aead)

    logging.info("Get encrypted dataset from S3")
    encrypted_dataset_dir = tempfile.TemporaryDirectory()
    get_from_s3(os.environ["S3_HOST"],
                os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"],
                os.environ["S3_BUCKET"],
                encrypted_dataset_dir.name,
                "S3_INSECURE" not in os.environ,
                objname="full_dataset.encrypted.zip")


def get_email_addresses(student_ids):
    if aead_primitive is None:
        raise Exception("missing decryption configuration")

    emails = []
    for id in student_ids:
        profile_info_json = aead_primitive.decrypt(
            open("%s/%d.profile.json" % (encrypted_dataset_dir.name, id), "rb").read(), b"")
        if json.loads(profile_info_json)["ritorno"]["indiMailIstituzionale"] != '':
            emails.append(json.loads(profile_info_json)["ritorno"]["indiMailIstituzionale"])
    return emails
