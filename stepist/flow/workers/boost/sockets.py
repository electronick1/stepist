"""
Add APP NAME FOR WORKERS (!!!)

"""

import random
import socket
import redis
import selectors
import asyncio

from threading import Thread

from stepist.flow.steps.step import StepData


DATA_HEADER = b'/stairs_line_separation/\n'


class SocketData:
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


class SocketBooster:

    def __init__(self, app, socket_address='/tmp/stairs', use_ipc=True,
                 socket_port_range=(49152, 65536), buffer_size=1000):

        self.app = app

        self.buffer_size = buffer_size
        self.use_ipc = use_ipc

        self.sender = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket_address = self.gen_address(socket_address, socket_port_range)
        self.sender.bind(self.socket_address)
        self.sender.listen()

        self.socket_redis = redis.Redis(**self.app.config.redis_kwargs)
        self._register_client_in_redis()

        self.connections = SocketConnections(self.sender,
                                             self.socket_redis,
                                             self.socket_address)

        self.receiver_event_loop = SocketWorkersEventLoop(self,
                                                          self.connections,
                                                          self.buffer_size)

        self.sender_event_loop = SocketProducerEventLoop(self,
                                                         self.connections,
                                                         self.buffer_size)
        self.cnt_job_sent = 0

        self.socket_connections_updater = \
            Thread(target=self.connections.connections_updater_loop,
                   args=(self.sender_event_loop, self.receiver_event_loop))
        self.socket_connections_updater.start()

    def gen_address(self, socket_address, socket_port_range):
        if self.use_ipc:
            return "%s%s" % (socket_address, random.randint(*socket_port_range))
        else:
            return "%s:%s" % (socket_address, random.randint(*socket_port_range))

    def _register_client_in_redis(self):
        key = "socket_client:%s" % self.socket_address
        self.socket_redis.client_setname(key)

    def send_job(self, step, data, **kwargs):
        self.sender_event_loop.send_job(step, data)

    def process(self, steps, die_on_error=True, die_when_empty=False):
        self.receiver_event_loop.run_event_loop()

    def handle_job(self, data_bytes):
        row_data = self.app.data_pickler.loads(data_bytes)

        s_data = SocketData.from_json(self.app, row_data)

        step = s_data.step
        step_data = s_data.step_data

        if step is not None:
            self.forward_to_queue(step, step_data)
            return

        # if step.step_key() not in available_handlers:
        #     self.forward_to_queue(step, step_data)

        s_data.step.receive_job(**s_data.step_data.get_dict())

    def forward_to_queue(self, step, step_data):
        self.app.add_job(step, step_data, skip_booster=True)


class SocketConnections:
    """
    Trying to update information about whole connections which we have
    in redis.

    """
    def __init__(self, socket_host, socket_redis, current_address):
        self.socket_host = socket_host
        self.socket_redis = socket_redis

        self.ignore_addresses = [current_address]

        self.current_workers_sockets = []

        self.current_producers = []
        self.current_producers_sockets = []
        self.producer_selector = selectors.DefaultSelector()

    def get_producers(self):
        workers_addresses = []

        clients_list = self.socket_redis.client_list()

        for client in clients_list:
            client_name = client['name']

            if 'socket_client:' not in client_name:
                continue

            address = client_name.split("socket_client:")[1]

            if address not in self.ignore_addresses:
                workers_addresses.append(address)

        return workers_addresses

    def connections_updater_loop(self, socket_producer, socker_receiver):
        while True:
            for producer_address in self.get_producers():
                if producer_address in self.current_producers:
                    continue

                producer_socket = socket.socket(socket.AF_UNIX,
                                                socket.SOCK_STREAM)
                producer_socket.connect(producer_address)
                producer_socket.setblocking(False)
                socker_receiver.add_new_producer(producer_socket)
                self.current_producers.append(producer_address)

            conn, addr = self.socket_host.accept()
            #conn.setblocking(1)
            conn.setblocking(False)
            self.current_workers_sockets.append(conn)
            socket_producer.add_new_worker(conn)


class SocketProducerEventLoop:

    def __init__(self, booster, socket_workers, buffer_size):
        self.booster = booster
        self.loop = asyncio.new_event_loop()
        self.socket_workers = socket_workers

        self.socket_buffer_size = []
        self.buffer_size = buffer_size

        self.workers_selector = selectors.DefaultSelector()
        self.sockets = []

    def add_new_worker(self, conn):
        self.socket_buffer_size.append(0)
        self.sockets.append(conn)
        self.workers_selector.register(conn,
                                       selectors.EVENT_READ,
                                       data=len(self.socket_buffer_size)-1)

    def send_job(self, step, step_data):

        if not self.socket_buffer_size:
            self.booster.forward_to_queue(step, step_data)
            return

        socket_data = SocketData(step=step, step_data=step_data)
        data_encoded = self.booster.app.data_pickler.dumps(socket_data.to_json())
        data_encoded = data_encoded.encode("utf-8")

        for i in range(len(self.socket_buffer_size)):
            if self.socket_buffer_size[i] > 0:
                try:
                    self.sockets[i].send(data_encoded + DATA_HEADER)

                except socket.timeout:
                    print("Timeout error for one of the worker,")
                    print("It will be removed from workers list")
                    # Disable socket buffer
                    self.socket_buffer_size[i] = -1
                    # continue send_job logic
                    self.booster.forward_to_queue(step, step_data)
                    return

                except BlockingIOError:
                    self.booster.forward_to_queue(step, step_data)
                    return

                except BrokenPipeError:
                    print("BrokenPipeError for one of the worker")
                    print("It will be removed from workers list")
                    self.socket_buffer_size[i] = -1
                    self.booster.forward_to_queue(step, step_data)
                    return

                self.socket_buffer_size[i] = self.socket_buffer_size[i] - 1
                return

        self.booster.forward_to_queue(step, step_data)

        events = self.workers_selector.select(timeout=5)

        for key, mask in events:
            index = key.data
            sock = key.fileobj

            try:
                d = sock.recv(1024)
            except socket.timeout as e:
                continue

            if d:
                self.socket_buffer_size[index] = self.buffer_size


class SocketWorkersEventLoop:

    def __init__(self, booster, socket_workers, buffer_size):
        self.booster = booster
        self.socket_workers = socket_workers

        self.socket_buffer_size = []
        self.buffer_size = buffer_size
        self.sockets = []

        self.threads_events = dict()

        self.producers_selector = selectors.DefaultSelector()

    def add_new_producer(self, producer_socket):
        self.sockets.append(producer_socket)
        self.socket_buffer_size.append(0)
        self.producers_selector.register(producer_socket,
                                         selectors.EVENT_READ,
                                         data=len(self.sockets)-1)

    def run_event_loop(self):
        while True:
            for i in range(len(self.sockets)):
                if self.socket_buffer_size[i] == 0:
                    try:
                        self.sockets[i].send(b'ready_to_consume')
                    except (socket.timeout, BlockingIOError):
                        continue
                    self.socket_buffer_size[i] = self.buffer_size

            events = self.producers_selector.select(timeout=5)

            for key, mask in events:
                index = key.data
                sock = key.fileobj

                data = b''
                try:
                    while True:
                        try:
                            sock_data = sock.recv(256 * 1024 * 1024)
                        except BlockingIOError:
                            break

                        data += sock_data
                        if not sock_data:
                            break

                except socket.timeout:
                    continue

                if data:
                    rows = data.split(DATA_HEADER)
                    self.socket_buffer_size[index] -= (len(rows) - 1)
