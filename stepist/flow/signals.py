from blinker import signal

before_step = signal("before_step")
after_step = signal("after_step")

flow_finished = signal("flow_finished")