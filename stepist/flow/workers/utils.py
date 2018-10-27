from stepist.flow import session


def validate_steps(steps):
    valid_steps = []

    for step in steps:
        if not step.as_worker:
            print(step.step_key(), "is not worker")
            continue

        if not session.get_step_by_key(step.step_key()):
            raise RuntimeError("step %s not registered" % step.step_key())

        if isinstance(step, str):
            valid_steps.append(session.get_step_by_key(step))
        else:
            valid_steps.append(step)

    return valid_steps
