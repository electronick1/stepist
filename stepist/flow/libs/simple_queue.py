import redis
import random
import uuid
import time

from ..config import setup_config, config
from stepist.flow import dbs


HANDLERS = {}


redis_dbs = []


def setup_redis_dbs(*r_dbs):
    global redis_dbs
    redis_dbs = r_dbs


def get_redis():
    global redis_dbs
    if not redis_dbs:
        return dbs.default_redis

    return random.choice(redis_dbs)


def setup(pickler=config.pickler, wait_timeout=5):
    setup_config(pickler=pickler,
                 wait_timeout=wait_timeout)


class ResultsReader(object):

    def __init__(self, key, request_id, results_count=None):
        self.r_key = key
        self.request_id = request_id
        self.results_count = results_count

        #self.pubsub = get_redis().pubsub()
        #self.pubsub.subscribe("%s:%s" % (self.r_key, self.request_id))

    def read(self):

        cnt_items = 0

        for item in self.pubsub.listen():
            if item['type'] != 'message':
                continue

            cnt_items += 1

            if cnt_items == self.results_count:
                self.pubsub.unsubscribe()

            yield config.pickler.loads(item['data'])


class ResultsWriter(object):
    def __init__(self, key, request_id):
        self.r_key = key
        self.request_id = request_id

    def write(self, data):
        get_redis().publish("%s:%s" % (self.r_key, self.request_id),
                             config.pickler.dumps(data))


def process(jobs, wait_time_for_job=1, die_when_empty=False):
    keys = list(jobs.keys())
    while True:
        key, data, writer = reserve_jobs(keys, wait_time_for_job)

        handler = jobs[key]

        if data is None:
            if die_when_empty:
                exit()
            continue

        try:
            handler.receive_job(data=data)
        except Exception:
            add_job(key, data)
            raise


def add_job(job_key, data, result_reader=None):

    if result_reader is None:
        result_reader = ResultsReader(redis_queue_key(job_key),
                                      str(uuid.uuid4()),
                                      0)

    data = {'request_id': result_reader.request_id,
            'data': data
            }

    get_redis().lpush(redis_queue_key(job_key),
                      config.pickler.dumps(data))

    result_reader.results_count += 1

    return result_reader


def reserve_jobs(job_keys, wait_timeout):

    try:
        job_data = get_redis().blpop(map(redis_queue_key, job_keys),
                                     timeout=wait_timeout,)
    except redis.exceptions.TimeoutError:
        return None, None, None

    if job_data is None:
        return None, None, None

    key = job_data[0].split("step_flow::job::")[1]
    job_data = config.pickler.loads(job_data[1])

    if not 'request_id' in job_data:
        return key, job_data, None

    writer = ResultsWriter(redis_queue_key(key),
                           job_data['request_id'])

    return key, job_data['data'], writer


def reserve_job(job_key):

    try:
        job_data = get_redis().lpop(redis_queue_key(job_key))
    except redis.exceptions.TimeoutError:
        return None, None

    if job_data is None:
        return None, None

    job_data = config.pickler.loads(job_data)

    if not 'request_id' in job_data:
        return job_data, None

    writer = ResultsWriter(redis_queue_key(job_key),
                           job_data['request_id'])

    return job_data['data'], writer


def flush_jobs(job_key):
    get_redis().delete(job_key)


# -- HELPERS --

def redis_queue_key(job_key):
    return "step_flow::job::%s" % job_key


