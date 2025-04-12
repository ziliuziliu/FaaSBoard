import redis
import logging
import ssl
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis/ElastiCache configuration
REDIS_HOST = "master.comm.hcdnu5.apse1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_PASSWORD = None
REDIS_DB = 0

def connect_to_redis():
    """Connect to Redis/ElastiCache"""
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
        logger.info("Successfully connected to Redis/ElastiCache (TLS)")
        return client
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis/ElastiCache: {e}")
        return None
    except Exception as e:
        logger.error(f"Error occurred while connecting to Redis/ElastiCache: {e}")
        return None

def list_key_names(redis_client):
    """List all key names"""
    if not redis_client:
        logger.error("Redis client is not initialized")
        return False

    try:
        logger.info("Starting to scan key names...")
        cursor = '0'
        total_keys = 0

        with tqdm(desc="Scanning progress") as pbar:
            while True:
                cursor, keys = redis_client.scan(cursor=cursor, count=1000)
                pbar.update(len(keys))
                for key in keys:
                    try:
                        key_str = key.decode('utf-8')
                        logger.info(f"{key_str}")
                        total_keys += 1
                    except Exception as e:
                        logger.warning(f"Error processing key: {e}")
                
                if cursor == 0:
                    break

        logger.info(f"Scan completed, found {total_keys} keys")
        return True

    except Exception as e:
        logger.error(f"Error occurred while scanning key names: {e}")
        return False

def main():
    redis_client = connect_to_redis()
    if not redis_client:
        return 1

    success = list_key_names(redis_client)
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())