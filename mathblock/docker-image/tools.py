def normalised(obj):
    return str(obj).lower()

def utf8len(s):
    return len(s.encode('utf-8'))

# Brakes the Bucket and the Object path of a MinIO bucket
def brake_minio_path(bucket_and_path):
    bucket_and_path_list = bucket_and_path.split("/")
    minio_bucket = bucket_and_path_list[0]
    del bucket_and_path_list[0]
    minio_object_path = "/".join(bucket_and_path_list)+"/"
    return [minio_bucket, minio_object_path]

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False