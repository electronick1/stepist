
JOBS = {}


def register_job(key, job_handler):
    global JOBS
    JOBS[key] = job_handler
