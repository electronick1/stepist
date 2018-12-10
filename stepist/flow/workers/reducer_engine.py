import time
from stepist.flow import session


class BaseReducerEngine(object):

    def add_job(self, reducer_step, data):
        raise NotImplemented()


class RedisReducerEngine(BaseReducerEngine):

    def __init__(self, app, redis_db, reducer_job_lifetime,
                 reducer_no_job_sleep_time):
        self.app = app
        self.redis_db = redis_db

        self.reducer_job_lifetime = reducer_job_lifetime
        self.reducer_no_job_sleep_time = reducer_no_job_sleep_time

    def add_job(self, reducer_step, data, **kwargs):
        hub_job_id = session.get_meta_data().get("hub_job_id", None)
        if hub_job_id is None:
            raise RuntimeError("job id not found. Do you have 'HUB' step before"
                               "reducer")

        current_amount = self.redis_db.zincrby(
            "%s:%s" % ("count", reducer_step.step_key()),
            1,
            hub_job_id,

        )

        pipe = self.redis_db.pipeline()
        pipe.hset(
            "%s:%s" % (reducer_step.step_key(), hub_job_id),
            current_amount,
            self.app.config.pickler.dumps(data),
        )
        pipe.expire(
            "%s:%s" % (reducer_step.step_key(), hub_job_id),
            self.reducer_job_lifetime
        )
        pipe.execute()

    def process(self, reducer_step):
        while True:
            max_value = self.redis_db.zpopmax(
                "%s:%s" % ("count", reducer_step.step_key()),
            )
            if not max_value:
                time.sleep(self.reducer_no_job_sleep_time)
                continue

            hub_job_id, count = max_value[0]
            hub_job_id = hub_job_id.decode("utf-8")

            key_count = hub_job_id.split(":")[1]

            if int(count) < int(key_count):
                self.redis_db.zincrby(
                    "%s:%s" % ("count", reducer_step.step_key()),
                    hub_job_id,
                    int(count)
                )
                time.sleep(self.reducer_no_job_sleep_time)
                continue

            data = self.redis_db.hgetall(
                "%s:%s" % (reducer_step.step_key(), hub_job_id),
            )
            if data:
                values = []
                for v in data.values():
                    v = self.app.config.pickler.loads(v.decode('utf-8'))
                    values.append(v)

                reducer_step(job_list=values)
                self.redis_db.delete("%s:%s" % (reducer_step.step_key(),
                                                hub_job_id))
