from stepist.flow import session

from .hub import Hub
from .reducer_step import ReducerStep


def call_next_step(data, next_step, **kwargs):

    next_step_handler = choose_next_step_handler(next_step)
    return next_step_handler(data, next_step, **kwargs)


def choose_next_step_handler(next_step):

    if next_step and isinstance(next_step, Hub):
        # WARNING! recursion here
        return init_next_hub_step

    if next_step.factory:
        return init_next_factory_step

    if next_step.as_worker:
        return init_next_worker_step
    else:
        return init_next_step


def init_next_hub_step(data, next_step):
    for next_step_item in next_step.steps:
        # WARNING! recursion here
        next_step_handler = choose_next_step_handler(next_step_item)
        next_step_handler(data, next_step_item)

        return None


def init_next_reducer_step(data, next_step):
    if isinstance(next_step, ReducerStep):
        next_step.add_data(data)
        return None


def init_next_worker_step(data, next_step, **kwargs):
    reader = next_step.add_job(data=data,
                               **kwargs)

    if next_step.wait_result:
        return reader.read()

    return reader


def init_next_factory_step(data, next_step):
    next_step.factory.add_data_iter(data)

    return None


def init_next_step(data, next_step):
    return next_step(**data)
