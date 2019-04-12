import time
import random
import zmq
import redis

from threading import Thread

from stepist.flow.steps.step import StepData


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
            _, address = client_name.split(":")
            if address not in self.ignore_addresses:
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

    def __init__(self, app, zmq_address, use_port=True,
                 zmq_port_range=(49152, 65536), zmq_context=None):
        self.app = app

        self.zmq_context = zmq_context or zmq.Context()

        self.sender = self.zmq_context.socket(zmq.PUSH)
        # we have 3 seconds to send message
        self.sender.setsockopt(zmq.SNDTIMEO, 3000)
        self.zmq_address = self._bind_zmq_sender(zmq_address,
                                                 use_port,
                                                 zmq_port_range)

        self.zmq_redis = redis.Redis(**self.app.config.redis_kwargs)
        self._register_zmq_client_in_redis()

        self.zmq_workers = ZMQWorkers(zmq_context=self.zmq_context,
                                      zmq_redis=self.zmq_redis,
                                      ignore_addresses=(self.zmq_address, ))
        self.zmq_updater = Thread(target=self.zmq_workers.zmq_updater_loop)
        self.zmq_updater.start()

    def _bind_zmq_sender(self, zmq_address, use_port, zmq_port_range):
        if use_port:
            port = self.sender.bind_to_random_port(zmq_address,
                                                   min_port=zmq_port_range[0],
                                                   max_port=zmq_port_range[1])
            return "%s:%s" % (zmq_address, port)
        else:
            self.sender.bind(zmq_address)
            return zmq_address

    def _register_zmq_client_in_redis(self):
        key = "zmq_client:%s" % self.zmq_address
        self.zmq_redis.client_setname(key)

    def send_job(self, step, data, **kwargs):
        zmq_data = ZMQData(step=step, step_data=data)

        try:
            self.sender.send_json(zmq_data.to_json())
        except zmq.error.Again:
            self.forward_to_queue(step, data)

    def process_job(self, possible_steps_keys):
        workers_pool = self.zmq_workers.poller.poll(timeout=10000)
        if not workers_pool:
            return False

        receiver = random.choice(workers_pool)

        step, step_data = None, None

        try:
            z_data = ZMQData.from_json(self.app,
                                       receiver.recv_json())

            step = z_data.step
            step_data = z_data.step_data

            if step.step_key() not in possible_steps_keys:
                self.forward_to_queue(step, step_data)
                raise RuntimeError("Step: %s - not found" % step.step_key())

            z_data.step.receive_job(**z_data.step_data)
        except Exception:
            if step is not None:
                self.forward_to_queue(step, step_data)
            raise

        return True

    def forward_to_queue(self, step, step_data):
        self.app.add_job(step, step_data)