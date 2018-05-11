from .session import register_step

from .steps import Step, ReducerStep, FactoryStep, Hub


def step(next_step, as_worker=False, wait_result=False):
    """
    Step decorator which initialize Step object, and register Step
    inside stepist


    :param next_step: next Step instance
    :param as_worker: True if it should be distribute
    :param wait_result: allow to return results in previous step
    :return:
    """

    def _wrapper(handler):

        step = Step(handler,
                    next_step,
                    as_worker=as_worker,
                    wait_result=wait_result)

        register_step(step)

        return step

    return _wrapper


def reducer_step():
    """
    ReducerStep decorator. We need this for aggregate all jobs results into one
    step. And also register step in global step list.

    In args you will get iterator which allow you go through all jobs results
    and process it.

    For example you can paste everything into AI model

    :return: ReducerStep instance
    """
    def _wrapper(handler):
        step = ReducerStep(handler)

        register_step(step)

        return step

    return _wrapper


def factory_step(next_step, as_worker=False):
    """
    Factory step decorator. If your step decorated by this function - your
    step should return iterator, and each item from this iter will be added
    to next step.

    :param next_step: Step instance
    :param as_worker: True if it should be distribute
    :return:
    """
    def _wrapper(handler):
        step = Step(handler,
                    next_step,
                    as_worker=as_worker,
                    wait_result=False)

        step.set_factory(FactoryStep(step))

        register_step(step)

        return step

    return _wrapper
