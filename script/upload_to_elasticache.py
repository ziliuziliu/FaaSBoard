import os
import sys
import redis
import logging
import ssl
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis/ElastiCache configuration
REDIS_HOST = "graph-hcdnu5.serverless.apse1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_PASSWORD = None
REDIS_DB = 0
REDIS_PREFIX = "faasboard:"
CHUNK_SIZE = 256 * 1024 * 1024  # 256 MB
PIPELINE_BATCH = 4  # Flush pipeline every 4 chunks


def connect_to_redis():
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            decode_responses=False,
            socket_timeout=60,
            socket_keepalive=True
        )
        client.ping()
        logger.info("Successfully connected to Redis/ElastiCache with TLS")
        return client
    except redis.ConnectionError as e:
        logger.error(f"Unable to connect to Redis/ElastiCache: {e}")
        return None
    except Exception as e:
        logger.error(f"Error connecting to Redis/ElastiCache: {e}")
        return None


def determine_content_type(file_path):
    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext in ['.txt', '.json', '.csv', '.meta']:
        return 'text'
    else:
        return 'binary'


def upload_large_file(redis_client, redis_key, file_path, hash_tag):
    try:
        with open(file_path, 'rb') as f:
            chunk_index = 0
            pipeline = redis_client.pipeline(transaction=False)
            hash_key = f"{REDIS_PREFIX}{{{hash_tag}}}:chunks"  # Store chunks in a hash

            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                chunk_field = f"chunk:{chunk_index}"
                pipeline.hset(hash_key, chunk_field, chunk)
                chunk_index += 1

                if chunk_index % PIPELINE_BATCH == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline(transaction=False)

            if chunk_index % PIPELINE_BATCH != 0:
                pipeline.execute()

            redis_client.hset(hash_key, "total_chunks", chunk_index)  # Store total chunk count in the hash
            logger.info(f"Uploaded large file in {chunk_index} chunks: {redis_key}")
    except Exception as e:
        logger.error(f"Error uploading large file {file_path}: {e}")


def upload_folder_to_elasticache(redis_client, local_folder_path):
    try:
        if not os.path.isdir(local_folder_path):
            logger.error(f"Folder does not exist: {local_folder_path}")
            return False

        file_count = 0
        parent_dir = os.path.dirname(local_folder_path)

        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, parent_dir)
                hash_tag = relative_path
                redis_key = f"{REDIS_PREFIX}{{{hash_tag}}}"
                content_type = determine_content_type(file)

                try:
                    file_size = os.path.getsize(local_file_path)
                    if file_size > CHUNK_SIZE:
                        upload_large_file(redis_client, redis_key, local_file_path, hash_tag)
                    elif content_type == 'text':
                        with open(local_file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        redis_client.set(redis_key, content)  # Store text content directly
                        logger.info(f"Uploaded file: {relative_path} -> {redis_key}")
                    else:
                        # For binary files, read and store them as bytes
                        with open(local_file_path, 'rb') as f:
                            content = f.read()
                        redis_client.set(redis_key, content)  # Store binary content directly
                        logger.info(f"Uploaded binary file: {relative_path} -> {redis_key}")
                    file_count += 1

                except UnicodeDecodeError:
                    logger.warning(f"Cannot decode as text, uploading as binary: {local_file_path}")
                    with open(local_file_path, 'rb') as f:
                        content = f.read()
                    redis_client.set(redis_key, content)  # Store binary content directly
                    file_count += 1
                except Exception as e:
                    logger.warning(f"Error processing file {local_file_path}: {e}")
                    continue

        logger.info(f"Successfully uploaded {file_count} files to ElastiCache")
        return True

    except Exception as e:
        logger.error(f"Error uploading data to ElastiCache: {e}")
        return False


def main():
    if len(sys.argv) != 2:
        print("Usage: python upload_to_elasticache.py <folder_name>")
        sys.exit(1)

    folder_name = sys.argv[1]
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    root_dir = script_dir.parent
    data_dir = root_dir / "data"
    target_folder_path = data_dir / folder_name

    redis_client = connect_to_redis()
    if not redis_client:
        sys.exit(1)

    success = upload_folder_to_elasticache(redis_client, str(target_folder_path))
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()