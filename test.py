from minio import Minio
from minio.error import S3Error

client = Minio("localhost:9000", "minioadmin", "minioadmin", secure=False)

bucket = "clickstream-lake"
prefix = "raw-events/event_date=2026-05-11/"

# List objects with the prefix; limit to 1 to just check existence
objects = client.list_objects(bucket, prefix=prefix, recursive=True)

# Convert generator to a list or check the first item
first_object = next(objects, None)

if first_object:
    print(f"✅ Partition exists and contains data.")
    
    # To get "metadata" for the partition, we usually aggregate the files
    total_size = 0
    file_count = 0
    latest_update = first_object.last_modified

    # Re-run list to aggregate (or do it in the first pass)
    all_files = client.list_objects(bucket, prefix=prefix, recursive=True)
    for obj in all_files:
        file_count += 1
        total_size += obj.size
        if obj.last_modified > latest_update:
            latest_update = obj.last_modified

    print(f"--- Partition Metadata Summary ---")
    print(f"Total Files: {file_count}")
    print(f"Total Size:  {total_size / (1024*1024):.2f} MB")
    print(f"Latest File: {latest_update}")
else:
    print("❌ Partition is empty or does not exist.")