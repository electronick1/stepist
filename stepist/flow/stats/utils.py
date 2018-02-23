

def concat_steps_to_str(steps):
    return '||'.join([step.step_key() for step in steps])
