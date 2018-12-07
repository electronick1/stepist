import redis
import uuid


HANDLERS = {}


class SimpleQueue:

    def __init__(self, pickler, redis_db):
        self.pickler = pickler
        self.redis_db = redis_db

    def process(self, jobs, wait_time_for_job=1, die_when_empty=False):
        keys = list(jobs.keys())
        while True:
            key, data, writer = self.reserve_jobs(keys, wait_time_for_job)

            if data is None:
                if die_when_empty:
                    exit()
                continue

            handler = jobs[key]

            try:
                handler.receive_job(**data)
            except Exception:
                self.add_job(key, data)
                raise

    def add_job(self, job_key, data, result_reader=None):

        if result_reader is None:
            result_reader = ResultsReader(self,
                                          self.redis_queue_key(job_key),
                                          str(uuid.uuid4()),
                                          0)

        data = {'request_id': result_reader.request_id,
                'data': data
                }

        self.redis_db.lpush(self.redis_queue_key(job_key),
                            self.pickler.dumps(data))

        result_reader.results_count += 1

        return result_reader

    def reserve_jobs(self, job_keys, wait_timeout):

        try:
            job_data = self.redis_db.blpop(map(self.redis_queue_key, job_keys),
                                           timeout=wait_timeout)

        except redis.exceptions.TimeoutError:
            return None, None, None

        if not job_data:
            return None, None, None

        key = job_data[0].decode("utf-8").split("step_flow::job::")[1]
        job_data = self.pickler.loads(job_data[1].decode("utf-8"))

        if not 'request_id' in job_data:
            return key, job_data, None

        writer = ResultsWriter(self,
                               self.redis_queue_key(key),
                               job_data['request_id'])

        return key, job_data['data'], writer

    def reserve_job(self, job_key):

        try:
            job_data = self.redis_db.lpop(self.redis_queue_key(job_key))
        except redis.exceptions.TimeoutError:
            return None, None

        if job_data is None:
            return None, None

        job_data = self.pickler.loads(job_data)

        if not 'request_id' in job_data:
            return job_data, None

        writer = ResultsWriter(self,
                               self.redis_queue_key(job_key),
                               job_data['request_id'])

        return job_data['data'], writer

    def flush_jobs(self, job_key):
        self.redis_db.delete(job_key)

    # -- HELPERS --

    def redis_queue_key(self, job_key):
        return "step_flow::job::%s" % job_key


class ResultsReader(object):

    def __init__(self, queue, key, request_id, results_count=None):
        self.queue = queue
        self.r_key = key
        self.request_id = request_id
        self.results_count = results_count

        #self.pubsub = get_redis().pubsub()
        #self.pubsub.subscribe("%s:%s" % (self.r_key, self.request_id))

    def read(self):

        cnt_items = 0

        for item in self.queue.redis_db.pubsub.listen():
            if item['type'] != 'message':
                continue

            cnt_items += 1

            if cnt_items == self.results_count:
                self.queue.redis_db.pubsub.unsubscribe()

            yield self.queue.pickler.loads(item['data'])


class ResultsWriter(object):
    def __init__(self, queue, key, request_id):
        self.queue = queue
        self.r_key = key
        self.request_id = request_id

    def write(self, data):
        self.queue.redis_db.publish("%s:%s" % (self.r_key, self.request_id),
                                    self.queue.pickler.dumps(data))





