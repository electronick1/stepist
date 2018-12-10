import threading

from contextlib import contextmanager


local = threading.local()


def storage():
    if not hasattr(local, 'storage'):
        local.storage = dict(
            steps={},
            steps_workers={},
            steps_listen_for_job={},
            meta_data={},
            flow_data={},
        )
    return local.storage


@contextmanager
def change_flow_ctx(meta_data, flow_data):
    old_meta = storage().get("meta_data")
    old_flow_data = storage().get("flow_data")

    try:
        set_meta_data(meta_data)
        set_flow_data(flow_data)
        yield
    finally:
        set_meta_data(old_meta)
        set_flow_data(old_flow_data)


def get_flow_data():
    return storage().get("flow_data", {})


def get_meta_data():
    return storage().get("meta_data", {})


def get_step_by_key(key):
    return get_steps().get(key, None)


def get_steps():
    return storage().get("steps", {})


def get_steps_workers():
    return storage().get("steps_workers", {})


def get_steps_to_listen():
    return storage().get("steps_listen_for_job", {})


def set_meta_data(meta_data):
    storage()['meta_data'] = meta_data


def set_flow_data(flow_data):
    storage()['flow_data'] = flow_data


def update_flow_data(flow_data):
    storage()['flow_data'].update(flow_data)


def update_meta_data(**meta_data):
    old_meta = get_meta_data()
    set_meta_data(dict(**old_meta, **meta_data))


def flush_session():
    global local
    local = threading.local()
