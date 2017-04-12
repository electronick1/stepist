import redis
import uuid

from .jobs import JOBS

from ..config import setup_config, config


def setup(redis=config.redis, pickler=config.pickler, wait_timeout=5):
    setup_config(redis=redis,
                 pickler=pickler,
                 wait_timeout=wait_timeout)


class ResultsReader(object):

    def __init__(self, key, request_id, results_count=None):
        self.r_key = key
        self.request_id = request_id
        self.results_count = results_count

        self.pubsub = config.redis.pubsub()
        self.pubsub.subscribe("%s:%s" % (self.r_key, self.request_id))

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
        config.redis.publish("%s:%s" % (self.r_key, self.request_id),
                             config.pickler.dumps(data))


def process(jobs, wait_time_for_job=1):
    while True:
        for job, executer in jobs.items():
            data, config, writer = reserve_job(job, wait_time_for_job)

            from ..step import CallConfig

            if data is None:
                continue

            writer.write(executer.execute_step(data=data,
                                               config=CallConfig.from_json(config)))


def add_job(job_key, data, call_config, result_reader=None):

    if result_reader is None:
        result_reader = ResultsReader(redis_queue_key(job_key),
                                      str(uuid.uuid4()),
                                      0)

    data = {'data': data,
            'call_config': call_config,
            'request_id': result_reader.request_id}

    config.redis.lpush(redis_queue_key(job_key),
                       config.pickler.dumps(data))

    result_reader.results_count += 1

    return result_reader


def reserve_job(job_key, wait_timeout):
    try:
        job_data = config.redis.blpop([redis_queue_key(job_key)],
                                      timeout=wait_timeout)
    except redis.exceptions.TimeoutError:
        return None, None, None

    if job_data is None:
        return None, None, None

    job_data = config.pickler.loads(job_data[1])

    writer = ResultsWriter(redis_queue_key(job_key),
                           job_data['request_id'])

    return job_data['data'], job_data['call_config'], writer


# -- HELPERS --

def has_jobs():
    for job in JOBS:
        if config.redis.llen(redis_queue_key(job)):
            return True

    return False


def redis_queue_key(job_key):
    return "step_flow::job::%s" % job_key


def run(*steps):

    for_process = {}

    print("WORKERS STARTED: ", steps)
    for step in steps:
        if not step.as_worker:
            continue

        if step.step_key() not in JOBS:
            raise RuntimeError("step %s not registered" % step)

        for_process[step.step_key()] = step

    process(for_process)


