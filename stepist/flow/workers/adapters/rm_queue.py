import pika
import ujson

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class RQAdapter(BaseWorkerEngine):
    def __init__(self, app, pika_connection=None):
        self.app = app
        self.pika_connection = pika_connection

        if not self.pika_connection:
            params = pika.ConnectionParameters(
                    host='localhost',
                    port=5672,
                )
            self.pika_connection = pika.BlockingConnection(parameters=params)
        
        self.channel_producer = self.pika_connection.channel()
        self.channel_consumer = self.pika_connection.channel()
        self.queues = dict()

    def add_job(self, step, data, **kwargs):
        json_data = ujson.dumps(data.get_dict())
        self.channel_producer.basic_publish(
            exchange='',
            routing_key=step.step_key(),
            body=json_data)

    def add_jobs(self, step, jobs_data, **kwargs):
        for job in jobs_data:
            self.add_job(step, job.get_dict(), **kwargs)

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        for step in steps:
            self.channel_consumer.basic_consume(
                StepReceiver(step),
                queue=step.step_key(),
                no_ack=True)

        self.channel_consumer.start_consuming()

    def flush_queue(self, step):
        self.channel_producer.queue_delete(queue=step.step_key())

    def jobs_count(self, *steps):
        sum_by_steps = 0

        for step in steps:
            sum_by_steps += self.queues[step.step_key()].method.message_count

        return sum_by_steps

    def register_worker(self, step):
        q = self.channel_producer.queue_declare(queue=step.step_key(),
                                                auto_delete=False,
                                                passive=True)

        self.queues[step.step_key()] = q

    def monitor_steps(self, step_keys, monitoring_for_sec):
        pass


class StepReceiver:
    def __init__(self, step):
        self.step = step

    def __call__(self, ch, method, properties, body):
        self.step.receive_job(**ujson.loads(body))

