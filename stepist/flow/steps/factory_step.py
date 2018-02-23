from .next_step import init_next_worker_step, init_next_step


class FactoryStep(object):
    """
    All to add jobs by iterator. Taking care about data iteration, and result
    reader.

    Able to return iterator, which can be used for reading flow result.

    """

    # current Step instance
    step = None

    # Reader object, for getting back results
    result_reader = None

    def __init__(self, step):
        self.step = step

        self.result_reader = None

    def add_data_iter(self, data_iter):
        """
        Getting data iterator, and put each item in queue

        :param data_iter: any data iterator object
        """
        for row_data in data_iter:
            if self.step.as_worker:
                init_next_worker_step(row_data,
                                      self.step,
                                      result_reader=self.result_reader)
            else:
                init_next_step(row_data,
                               self.step)

    def result(self):
        """
        :return: iterator
        """
        return self.result_reader.read()
