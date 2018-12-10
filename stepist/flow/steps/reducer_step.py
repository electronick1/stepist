from stepist.flow.steps import Step


class ReducerStep(Step):

    def __init__(self, app, handler):
        self.app = app
        self.handler = handler

        super(ReducerStep, self).__init__(app,
                                          handler,
                                          None,
                                          as_worker=True,
                                          wait_result=False)

    @property
    def __name__(self):
        return self.handler.__name__

    def add_job(self, data, **kwargs):
        self.app.reducer_engine.add_job(self, data, **kwargs)

    def step_key(self):
        return "reducer_step::%s" % self.__name__
