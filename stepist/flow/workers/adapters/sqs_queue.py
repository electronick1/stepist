import boto3
import ujson
import random

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class SQSAdapter(BaseWorkerEngine):
    def __init__(self, app, session=boto3, visibility_timeout=None,
                 message_retention_period=None, wait_seconds=5):
        self.app = app

        self.sqs_client = session.client('sqs')
        self.sqs_resource = session.resource('sqs')

        self.message_retention_period = message_retention_period
        self.visibility_timeout = visibility_timeout
        self.wait_seconds = wait_seconds

        self._queues = dict()
        self._steps = dict()

    def add_job(self, step, data, **kwargs):
        queue = self._queues.get(step.step_key(), None)
        if not queue:
            raise RuntimeError("Queue %s not found" % step.step_key())

        kwargs = {
            'MessageBody': ujson.dumps(data.get_dict()),
            'MessageAttributes': {},
            'DelaySeconds': 0
        }

        ret = queue.send_message(**kwargs)
        return ret['MessageId']

    def add_jobs(self, step, jobs_data, **kwargs):
        for job_data in jobs_data:
            self.add_job(step, job_data.get_dict(), **kwargs)

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        queues = list(self._queues.keys())

        if not queues:
            return

        while True:
            random.shuffle(queues)

            step_key = queues[0]
            queue = self._queues[step_key]

            kwargs = {
                'WaitTimeSeconds': self.wait_seconds,
                'MaxNumberOfMessages': 10,
                'MessageAttributeNames': ['All'],
                'AttributeNames': ['All'],
            }
            messages = queue.receive_messages(**kwargs)

            msg_results = []
            for msg in messages:
                data = ujson.loads(msg.body)
                try:
                    self._steps[step_key].receive_job(**data)
                except Exception:
                    if die_on_error:
                        raise

                msg_results.append({
                    'Id': msg.message_id,
                    'ReceiptHandle': msg.receipt_handle
                })

            if msg_results:
                queue.delete_messages(Entries=msg_results)

    def flush_queue(self, step):
        pass

    def jobs_count(self, *steps):
        pass

    def register_worker(self, step):
        attrs = {}
        kwargs = {
            'QueueName': step.step_key(),
            'Attributes': attrs,
        }
        if self.message_retention_period is not None:
            attrs['MessageRetentionPeriod'] = str(self.message_retention_period)
        if self.visibility_timeout is not None:
            attrs['VisibilityTimeout'] = str(self.visibility_timeout)

        self.sqs_client.create_queue(**kwargs)

        queue = self.sqs_resource.get_queue_by_name(QueueName=step.step_key())

        self._queues[step.step_key()] = queue
        self._steps[step.step_key()] = step

    def monitor_steps(self, step_keys, monitoring_for_sec):
        pass


class StepReceiver:
    def __init__(self, step):
        self.step = step

    def __call__(self, ch, method, properties, body):
        self.step.receive_job(**ujson.loads(body))


