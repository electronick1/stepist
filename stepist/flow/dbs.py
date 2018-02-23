

redis_stats_db = None


def get_redis_stats():
    global redis_stats_db
    return redis_stats_db


def set_redis_stats_db(redis_instance):
    global redis_stats_db
    if redis_stats_db:
        redis_stats_db.connection_pool.disconnect()
    redis_stats_db = redis_instance
