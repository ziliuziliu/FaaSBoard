import redis
import logging
import ssl
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REDIS_HOST = "master.comm.hcdnu5.apse1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_PASSWORD = None
REDIS_DB = 0

# keys/patterns you need to reserve
KEEP_PATTERNS = [
    "GlobalIPList",
    "GlobalIPList_.*",
]

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
        logger.info("connect to Redis/ElastiCache (TLS)")
        return client
    except redis.ConnectionError as e:
        logger.error(f"can't connect to Redis/ElastiCache: {e}")
        return None
    except Exception as e:
        logger.error(f"error when trying connect to Redis/ElastiCache: {e}")
        return None

def should_keep_key(key):
    key_str = key.decode('utf-8')
    for pattern in KEEP_PATTERNS:
        if pattern.endswith('.*'):
            if key_str.startswith(pattern[:-2]):
                return True
        elif key_str == pattern:
            return True
    return False

def delete_unwanted_keys(redis_client):
    if not redis_client:
        logger.error("Redis not initialized")
        return False
    
    try:
        keys_to_delete = []
        cursor = '0'
        total_keys = 0
        
        logger.info("start scan...")
        with tqdm(desc="scan schedule") as pbar:
            while True:
                cursor, keys = redis_client.scan(cursor=cursor, count=1000)
                total_keys += len(keys)
                pbar.update(len(keys))
                
                for key in keys:
                    if not should_keep_key(key):
                        keys_to_delete.append(key)
                
                if cursor == 0:
                    break
        
        logger.info(f"scan finished, find {total_keys} keys")
        logger.info(f"need to delete {len(keys_to_delete)} keys")
        
        if not keys_to_delete:
            logger.info("no keys need to delete")
            return True
        
        batch_size = 500
        deleted_count = 0
        
        logger.info("start delete...")
        with tqdm(total=len(keys_to_delete), desc="delete schedule") as pbar:
            for i in range(0, len(keys_to_delete), batch_size):
                batch = keys_to_delete[i:i + batch_size]
                for key in batch:
                    try:
                        redis_client.delete(key)
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"error when deleting {key} : {e}")
                pbar.update(len(batch))
        
        logger.info(f"successfully deleted {deleted_count} keys")
        return True
        
    except Exception as e:
        logger.error(f"error when deleting keys : {e}")
        return False

def main():
    logger.info("keeps:")
    for pattern in KEEP_PATTERNS:
        logger.info(f" - {pattern}")
    
    redis_client = connect_to_redis()
    if not redis_client:
        return 1
    
    success = delete_unwanted_keys(redis_client)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)