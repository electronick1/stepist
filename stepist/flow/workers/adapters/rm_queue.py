import pika
import ujson

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class RQAdapter(BaseWorkerEngine):
    def __init__(self, pika_connection=None):
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
        queue_name = self.get_queue_name(step)
        json_data = ujson.dumps(data.get_dict())
        self.channel_producer.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json_data)

    def add_jobs(self, step, jobs_data, **kwargs):
        for job in jobs_data:
            self.add_job(step, job.get_dict(), **kwargs)

    def receive_job(self, step):
        pass

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        for step in steps:
            queue_name = self.get_queue_name(step)
            self.channel_consumer.basic_consume(
                StepReceiver(step),
                queue=queue_name,
                no_ack=True)

        self.channel_consumer.start_consuming()

    def flush_queue(self, step):
        queue_name = self.get_queue_name(step)
        self.channel_producer.queue_delete(queue=queue_name)

    def jobs_count(self, *steps):
        sum_by_steps = 0

        for step in steps:
            queue_name = self.get_queue_name(step)
            sum_by_steps += self.queues[queue_name].method.message_count

        return sum_by_steps

    def register_worker(self, step):
        queue_name = self.get_queue_name(step)
        q = self.channel_producer.queue_declare(queue=queue_name,
                                                auto_delete=False,
                                                passive=True)

        self.queues[queue_name] = q

    def monitor_steps(self, step_keys, monitoring_for_sec):
        pass

    def get_queue_name(self, step):
        return step.step_key()


class StepReceiver:
    def __init__(self, step):
        self.step = step

    def __call__(self, ch, method, properties, body):
        self.step.receive_job(**ujson.loads(body))

