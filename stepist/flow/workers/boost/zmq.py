import time
import random
import zmq
import redis
import ujson

from threading import Thread

from stepist.flow.steps.step import StepData
from tqdm import tqdm


class ZMQData:
    def __init__(self, step, step_data):
        self.step = step
        self.step_data = step_data

    def to_json(self):
        return dict(
            step_key=self.step.step_key(),
            step_data=self.step_data.get_dict(),
        )

    @classmethod
    def from_json(cls, app, json_data):
        return cls(
            step=app.steps.get(json_data['step_key']),
            step_data=StepData(**json_data['step_data'])
        )


class ZMQWorkers:
    def __init__(self, zmq_context, zmq_redis, ignore_addresses):
        self.zmq_context = zmq_context
        self.zmq_redis = zmq_redis
        self.ignore_addresses = ignore_addresses

        self.current_workers = []

        self.poller = zmq.Poller()

    def update_poller(self):
        for worker_address in self.get_workers():
            if worker_address in self.current_workers:
                continue

            socket_pull = self.zmq_context.socket(zmq.PULL)
            socket_pull.connect(worker_address)
            self.poller.register(socket_pull, zmq.POLLIN)

            self.current_workers.append(worker_address)

    def get_workers(self):
        workers_addresses = []

        clients_list = self.zmq_redis.client_list()

        for client in clients_list:
            client_name = client['name']

            if 'zmq_client:' not in client_name:
                continue

            address = client_name.split("zmq_client:")[1]

            #if address not in self.ignore_addresses:
            workers_addresses.append(address)

        return workers_addresses

    def zmq_updater_loop(self):
        while True:
            if not self.current_workers:
                self.update_poller()
                time.sleep(5)
            else:
                self.update_poller()
                time.sleep(10)


class ZMQBooster:

    def __init__(self, app, zmq_address='/tmp/stairs', use_ipc=True,
                 zmq_port_range=(49152, 65536), zmq_context=None,
                 buffer_size=10**4):

        self.app = app

        self.buffer_size = buffer_size
        self.use_ipc = use_ipc

        self.zmq_context = zmq_context or zmq.Context()

        self.sender = self.zmq_context.socket(zmq.PUSH)
        # we have 3 seconds to send message
        self.sender.setsockopt(zmq.SNDTIMEO, 10000)

        self.zmq_address = self.gen_address(zmq_address, zmq_port_range)
        self.sender.bind(self.zmq_address)

        self.zmq_redis = redis.Redis(**self.app.config.redis_kwargs)
        self._register_zmq_client_in_redis()

        self.zmq_workers = ZMQWorkers(zmq_context=self.zmq_context,
                                      zmq_redis=self.zmq_redis,
                                      ignore_addresses=(self.zmq_address, ))

        self.zmq_backward_processing = self.zmq_context.socket(zmq.PULL)
        self.zmq_backward_processing.connect(self.zmq_address)

        self.cnt_job_sent = 0

        self.zmq_updater = Thread(target=self.zmq_workers.zmq_updater_loop)
        self.zmq_updater.start()

    def gen_address(self, zmq_address, zmq_port_range):
        if self.use_ipc:
            return "%s%s" % (zmq_address, random.randint(*zmq_port_range))
        else:
            return "%s:%s" % (zmq_address, random.randint(*zmq_port_range))

    def _register_zmq_client_in_redis(self):
        key = "zmq_client:%s" % self.zmq_address
        self.zmq_redis.client_setname(key)

    def send_job(self, step, data, **kwargs):
        self.cnt_job_sent += 1

        zmq_data = ZMQData(step=step, step_data=data)

        self.sender.send(ujson.dumps(zmq_data.to_json()).encode('utf-8'))

        if self.cnt_job_sent % self.buffer_size == 0:
            self.cleanup_backlog()

    def cleanup_backlog(self):
        for i in range(self.buffer_size):
            try:
                zmq_row_data = self.zmq_backward_processing.recv(zmq.NOBLOCK)
            except zmq.error.Again:
                # queue is empty
                return None
            if not zmq_row_data:
                return None

            z_data = ZMQData.from_json(self.app, ujson.loads(zmq_row_data))
            self.forward_to_queue(step=z_data.step,
                                  step_data=z_data.step_data)

    def process(self, steps, die_on_error=True, die_when_empty=False):
        steps_keys = [step.step_key() for step in steps]
        while True:
            self.process_job(steps_keys)

    def process_job(self, possible_steps_keys):
        workers_pool = self.zmq_workers.poller.poll(timeout=10000)
        if not workers_pool:
            self.zmq_workers.update_poller()
            return False

        receiver = random.choice(workers_pool)[0]

        step, step_data = None, None

        # multiple by 10 because of performance reason
        for _ in tqdm(range(self.buffer_size * 10)):
            try:

                zmq_row_data = ujson.loads(receiver.recv(zmq.NOBLOCK))

                z_data = ZMQData.from_json(self.app, zmq_row_data)

                step = z_data.step
                step_data = z_data.step_data

                if step.step_key() not in possible_steps_keys:
                    self.forward_to_queue(step, step_data)
                    raise RuntimeError("Step: %s - not found" % step.step_key())

                z_data.step.receive_job(**z_data.step_data.get_dict())

            except zmq.error.Again:
                break
            except Exception:
                if step is not None:
                    self.forward_to_queue(step, step_data)
                raise

        return True

    def forward_to_queue(self, step, step_data):
        self.app.add_job(step, step_data, skip_booster=True)
