def _secret(*keys):
    for key in keys:
        try:
            return dbutils.secrets.get(scope="aws", key=key)
        except Exception:
            continue
    raise RuntimeError(f"Missing AWS secret for any of: {', '.join(keys)}")


AWS_ACCESS_KEY_ID = _secret("aws_access_key_id", "aws_access_key")
AWS_SECRET_ACCESS_KEY = _secret("aws_secret_access_key", "aws_secret_key")

try:
    AWS_SESSION_TOKEN = _secret("aws_session_token")
except Exception:
    AWS_SESSION_TOKEN = None

AWS_BUCKET_NAME = "choc-rady-clinical-bronze-demo"
MOUNT_NAME = "/mnt/mri_landing_zone"

if any(mount.mountPoint == MOUNT_NAME for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(MOUNT_NAME)

extra_configs = {
    "fs.s3a.access.key": AWS_ACCESS_KEY_ID,
    "fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
}
if AWS_SESSION_TOKEN:
    extra_configs["fs.s3a.session.token"] = AWS_SESSION_TOKEN

try:
    dbutils.fs.mount(
        source=f"s3a://{AWS_BUCKET_NAME}",
        mount_point=MOUNT_NAME,
        extra_configs=extra_configs,
    )
    print(f"Mounted {AWS_BUCKET_NAME} at {MOUNT_NAME}")
    display(dbutils.fs.ls(f"{MOUNT_NAME}/raw/"))
except Exception as exc:
    print(f"Mount failed: {exc}")