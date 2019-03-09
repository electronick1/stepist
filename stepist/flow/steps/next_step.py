import copy

from .hub import Hub


def call_next_step(data, next_step, batch_data=False, **kwargs):

    next_step_handler = choose_next_step_handler(next_step)
    return next_step_handler(data, next_step, batch_data=batch_data, **kwargs)


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


def init_next_hub_step(data, hub_step, batch_data=False):
    """
    WARNING: data copping happens here
    """
    if batch_data:
        raise RuntimeError("It's not possible to use batch in hub step")

    if isinstance(data, list):
        if len(data) != len(hub_step.steps):
            raise RuntimeError("Amount of data not equal to amount of steps")
        data_list = data
    else:
        data_list = [data for _ in hub_step.steps]

    hub_step.update_meta()

    hub_result = None

    for i, next_step_item in enumerate(hub_step.steps):
        # WARNING! recursion here
        data = data_list[i]
        next_step_handler = choose_next_step_handler(next_step_item)
        result = next_step_handler(copy.deepcopy(data), next_step_item)

        if hub_result is None:
            hub_result = result
        else:
            hub_result.update(result)

    return hub_result


def init_next_reducer_step(data, next_step, batch_data=False):
    from .reducer_step import ReducerStep

    if batch_data:
        raise RuntimeError("It's not possible to use batch in reducer step")

    if isinstance(next_step, ReducerStep):
        next_step.add_job(data)
        return None


def init_next_worker_step(data, next_step, batch_data=False, **kwargs):
    if batch_data:
        return next_step.add_jobs(jobs_data=data)

    return next_step.add_job(data=data, **kwargs)


def init_next_factory_step(data, next_step, batch_data=False):
    if batch_data:
        raise RuntimeError("It's not possible to use batch in factory step")

    next_step.factory.add_data_iter(data)

    return None


def init_next_step(data, next_step, batch_data=False):
    return next_step(**data)

