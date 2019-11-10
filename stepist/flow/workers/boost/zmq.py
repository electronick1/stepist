"""
Add APP NAME FOR WORKERS (!!!)

"""

import time
import random
import zmq
import redis
import queue

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
    def __init__(self, zmq_context, zmq_redis, ignore_addresses, buffer_size):
        self.zmq_context = zmq_context
        self.zmq_redis = zmq_redis
        self.ignore_addresses = ignore_addresses

        self.buffer_size = buffer_size

        self.current_workers = []
        self.current_sockets = []

        self.poller = zmq.Poller()

    def update_poller(self):
        online_workers = self.get_workers()

        for worker_address in online_workers:
            if worker_address in self.current_workers:
                continue

            socket_pull = self.zmq_context.socket(zmq.PULL)
            socket_pull.setsockopt(zmq.RCVTIMEO, 500)
            socket_pull.setsockopt(zmq.RCVHWM, self.buffer_size)
            socket_pull.connect(worker_address)
            self.poller.register(socket_pull, zmq.POLLIN)

            self.current_workers.append(worker_address)
            self.current_sockets.append(socket_pull)

        for addr in tuple(self.current_workers):
            if addr not in online_workers:
                i = self.current_workers.index(addr)
                del self.current_workers[i]
                self.poller.unregister(self.current_sockets[i])
                del self.current_sockets[i]

    def get_workers(self):
        workers_addresses = []

        clients_list = self.zmq_redis.client_list()

        for client in clients_list:
            client_name = client['name']

            if 'zmq_client:' not in client_name:
                continue

            address = client_name.split("zmq_client:")[1]

            if address not in self.ignore_addresses:
                workers_addresses.append(address)

        return workers_addresses

    def zmq_updater_loop(self):
        while True:
            if not self.current_workers:
                self.update_poller()
            else:
                self.update_poller()

            time.sleep(5)


class ZMQBooster:

    def __init__(self, app, zmq_address='/tmp/stairs', use_ipc=True,
                 zmq_port_range=(49152, 65536), zmq_context=None,
                 buffer_size=1):

        self.app = app

        self.buffer_size = buffer_size
        self.use_ipc = use_ipc

        self.zmq_context = zmq_context or zmq.Context()



        self.sender = ZMQSender()
        self.receiver = ZMQReceiver()





        self.zmq_redis = redis.Redis(**self.app.config.redis_kwargs)
        self._register_zmq_client_in_redis()

        self.zmq_workers = ZMQWorkers(zmq_context=self.zmq_context,
                                      zmq_redis=self.zmq_redis,
                                      ignore_addresses=(self.zmq_address, ),
                                      buffer_size=self.buffer_size)

        self.zmq_backward_processing = self.zmq_context.socket(zmq.PULL)
        self.zmq_backward_processing.setsockopt(zmq.RCVTIMEO, 500)
        self.zmq_backward_processing.setsockopt(zmq.RCVHWM, 1)
        self.zmq_backward_processing.connect(self.zmq_address)

        self.cnt_job_sent = 0

        self.zmq_updater = Thread(target=self.zmq_workers.zmq_updater_loop)
        self.zmq_updater.start()

    def gen_address(self, zmq_address, zmq_port_range):
        if self.use_ipc:
            bind_addr = "ipc://%s%s" % (zmq_address, random.randint(*zmq_port_range))
            print(bind_addr)
            return bind_addr
        else:
            return "%s:%s" % (zmq_address, random.randint(*zmq_port_range))

    def _register_zmq_client_in_redis(self):
        key = "zmq_client:%s" % self.zmq_address
        self.zmq_redis.client_setname(key)

    def send_job(self, step, data, **kwargs):

        if not self.zmq_workers.current_workers:
            self.forward_to_queue(step, data)
            return

        zmq_data = ZMQData(step=step, step_data=data)



    def send_jobs(self):

        data_encoded = self.app.data_pickler.dumps(zmq_data.to_json())

        if isinstance(data_encoded, str):
            self.sender.send_string(data_encoded)
        else:
            self.sender.send(data_encoded)

        # if self.cnt_job_sent and self.cnt_job_sent % self.buffer_size == 0:
        #     self.cleanup_backlog()

    def process(self, steps, die_on_error=True, die_when_empty=False):
        steps_keys = [step.step_key() for step in steps]
        while True:
            self.process_job(steps_keys)

    def process_job(self, possible_steps_keys):
        workers_pool = self.zmq_workers.poller.poll(timeout=10000)
        if not workers_pool:
            self.zmq_workers.update_poller()
            return False

        step, step_data = None, None

        # multiple by 10 because of performance reason
        for receiver, _ in workers_pool:
            try:
                zmq_row_data = self.app.data_pickler.loads(receiver.recv())

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


class ZMQSender:

    def __init__(self, zmq_context, zmq_addr, farward_to_queue_handler):
        self.sender = zmq_context.socket(zmq.PUSH | zmq.PULL)
        # we have 3 seconds to send message
        self.sender.setsockopt(zmq.SNDTIMEO, 100)
        self.sender.setsockopt(zmq.SNDHWM, 100)

        self.zmq_addr = zmq_addr
        self.sender.bind(self.zmq_addr)

        self.farward_to_queue_handler = farward_to_queue_handler

        self.max_size = 1000
        self.jobs_queue = queue.Queue(maxsize=self.max_size)

        self.start_jobs_sending()

    def start_jobs_sending(self):
        while True:
            data_batch = []

            while True:
                try:
                    data_batch.append(self.jobs_queue.get(block=False))
                    if data_batch >= self.max_size:
                        break
                except queue.Empty:
                    break

            if data_batch:
                self.sender.send(data_batch)
                ok_signal = self.sender.recv(100)
                if not ok_signal:
                    for row in data_batch:
                        self.farward_to_queue_handler(row)

    def add_job(self, data):
        try:
            self.jobs_queue.put(data, block=True, timeout=1)
        except queue.Full:
            self.farward_to_queue_handler(data)


