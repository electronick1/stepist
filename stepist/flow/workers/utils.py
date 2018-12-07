from stepist.flow import session


def validate_steps(steps):
    valid_steps = []

    for step in steps:
        if not step.as_worker:
            print(step.step_key(), "is not worker")
            continue

        if isinstance(step, str):
            valid_steps.append(session.get_step_by_key(step))
        else:
            valid_steps.append(step)

    return valid_steps
