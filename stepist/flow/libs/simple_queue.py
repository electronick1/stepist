import random
import redis
import time


HANDLERS = {}


class SimpleQueue:

    def __init__(self, pickler, redis_db):
        self.pickler = pickler
        self.redis_db = redis_db

    def process(self, jobs, wait_time_for_job=1, die_when_empty=False,
                die_on_error=True, verbose=False):
        keys = list(jobs.keys())

        jobs_processed_before_empty = 0
        time_started_before_empty = time.time()

        while True:
            key, data = self.reserve_jobs(keys, wait_time_for_job)

            if data is None:
                if verbose and jobs_processed_before_empty:
                    delta_time = round(time.time() - time_started_before_empty, 3)
                    print("No more jobs in queues. Processed %s jobs in %s sec." %
                          (jobs_processed_before_empty, delta_time))

                jobs_processed_before_empty = 0
                time_started_before_empty = time.time()

                if die_when_empty:
                    exit()

                continue

            jobs_processed_before_empty += 1
            handler = jobs[key]

            try:
                handler.receive_job(**data)
            except Exception:
                self.add_job(key, data)
                if die_on_error:
                    raise

    def add_job(self, job_key, data):
        data = self.pickler.dumps({'data': data})
        self.redis_db.lpush(self.redis_queue_key(job_key), data)

    def add_jobs(self, job_key, jobs_data):
        pipe = self.redis_db.pipeline()

        for job_data in jobs_data:
            data = self.pickler.dumps({'data': job_data})
            pipe.lpush(self.redis_queue_key(job_key), data)

        pipe.execute()

    def reserve_jobs(self, job_keys, wait_timeout):
        random.shuffle(job_keys)
        try:
            job_data = self.redis_db.blpop(map(self.redis_queue_key, job_keys),
                                           timeout=wait_timeout)

        except redis.exceptions.TimeoutError:
            return None, None

        if not job_data:
            return None, None


        key = job_data[0].split("stepist::job::")[1]
        job_data = self.pickler.loads(job_data[1])

        return key, job_data['data']

    def reserve_job(self, job_key):

        try:
            job_data = self.redis_db.lpop(self.redis_queue_key(job_key))
        except redis.exceptions.TimeoutError:
            return None

        if job_data is None:
            return None

        job_data = self.pickler.loads(job_data)
        return job_data['data']

    def flush_jobs(self, job_key):
        self.redis_db.delete(job_key)

    # -- HELPERS --

    def redis_queue_key(self, job_key):
        return "stepist::job::%s" % job_key



