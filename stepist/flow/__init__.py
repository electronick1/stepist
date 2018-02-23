from .steps.step import StepData
from .steps.next_step import call_next_step
from .step import step, reducer_step, factory_step
from .workers import worker_engine, simple_multiprocessing
from .session import get_step_by_key
from .config import setup_config
from .utils import StopFlowFlag


def run(*steps):
    return workers.process(*steps)


def just_do_it(workers_count, *args, _warning=True,  **kwargs):
    if _warning:
        print("You are using python multiprocessing for workers,"
              "do NOT do it in production\n")
    simple_multiprocessing(workers_count, *args, **kwargs)
