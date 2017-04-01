import redis
import uuid

from .jobs import JOBS

from ..config import config


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
    global STEPS

    while True:
        for job, executer in jobs.items():
            data, last_step, writer = reserve_job(job, wait_time_for_job)

            if data is None:
                continue

            writer.write(executer.execute_step(data=data,
                                               last_step=last_step,
                                               reducer_step=None))


def add_job(job_key, data_rows, last_step):

    request_id = str(uuid.uuid4())

    result_reader = ResultsReader(redis_queue_key(job_key),
                                  request_id,
                                  len(data_rows))

    for data in data_rows:
        data = {'data': data,
                'last_step': last_step,
                'request_id': request_id}

        config.redis.lpush(redis_queue_key(job_key),
                           config.pickler.dumps(data))

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

    return job_data['data'], job_data['last_step'], writer


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


