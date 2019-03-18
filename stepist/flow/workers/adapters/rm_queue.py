import pika
import ujson
import random

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class RQAdapter(BaseWorkerEngine):
    def __init__(self, pika_params=None):
        if not pika_params:
            self.params = pika.ConnectionParameters(
                host='localhost',
                port=5672,
            )
        else:
            self.params = pika_params

        self.pika_connection = pika.BlockingConnection(parameters=self.params)
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
            self.add_job(step, job, **kwargs)

    def receive_job(self, step):
        pass

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        # Pika is not thread safe we need to create new connection per thread
        channel = self.channel_consumer
        receivers = [StepReceiver(step, channel) for step in steps]

        empty_count = 0

        while True:
            random.shuffle(receivers)

            r = receivers[0]
            q = r.step.get_queue_name()
            result = channel.basic_get(queue=q, no_ack=False)

            if result and result[0] and result[2]:
                r(*result)
                empty_count = 0
            else:
                empty_count += 1
                if empty_count > len(receivers) * 3 and die_when_empty:
                    exit()

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
                                                durable=True)

        self.queues[queue_name] = q

    def monitor_steps(self, step_keys, monitoring_for_sec):
        pass

    def get_queue_name(self, step):
        return step.step_key()


class StepReceiver:
    def __init__(self, step, channel):
        self.step = step
        self.channel = channel

    def __call__(self, method, properties, body):
        self.step.receive_job(**ujson.loads(body))
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
