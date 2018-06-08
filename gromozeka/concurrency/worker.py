import functools
import logging
import multiprocessing
import queue
import threading
import traceback

from gromozeka.exceptions import Retry, MaxRetriesExceedException
from gromozeka.primitives import Task


class Worker:
    """Worker class for `gromozeka.concurrency.Pool`. Uses as mixin.

    Attributes:
        logger(logging.Logger): class logger
        pool(gromozeka.concurrency.Pool): parent pool instance
        _stop_event(multiprocessing.Event or threading.Event)

    Args:
        pool(gromozeka.concurrency.Pool): Parent pool instance
    """

    name = None  # multiprocessing or threading part
    ident = None  # multiprocessing or threading part

    def __init__(self, pool):
        self.logger = logging.getLogger("gromozeka.pool.worker")
        self.pool = pool
        self._stop_event = None

    def run(self):
        """Run worker in thread or process

        Raises:
            exceptions.MaxRetriesExceedException:
            BaseException:
        """
        self.logger.info("start")
        exc = None
        while not self._stop_event.is_set():
            try:
                # get request object from queue
                task_proto = self.pool.worker_queue.get(True, 0.05)
            except queue.Empty:
                continue
            # make Task object from request
            task = Task.from_proto(task_proto)
            try:
                res = functools.partial(task.func, *(task, *task.args) if task.bind else task.args, **task.kwargs)()
                task.on_success(res)
            except Retry as e:
                task.error = str(e)
                task.exc = traceback.format_stack()
                if task.on_retry(e):
                    continue
                task.on_fail()
                self.pool.remove_worker(self.ident)
                exc = MaxRetriesExceedException()
                break
            except Exception as e:
                task.error = str(e)
                task.exc = traceback.format_exc()
                task.on_fail()
                self.pool.remove_worker(self.ident)
                exc = e
                break
        if exc is not None:
            raise exc
        self.logger.info("stop")

    def stop(self):
        """Stop worker. multiprocessing or threading part

        """
        pass

    def is_alive(self):
        """Check worker is alive. multiprocessing or threading part

        Returns:
            bool: True if worker is alive or False
        """
        pass


class ProcessWorker(multiprocessing.Process, Worker):
    """See `gromozeka.concurrency.Worker` documentation

    Attributes:
        _stop_event(multiprocessing.Event): Stop event
    """
    __doc__ = Worker.__doc__

    def __init__(self, pool):
        multiprocessing.Process.__init__(self)
        Worker.__init__(self, pool=pool)
        self._stop_event = multiprocessing.Event()

    def run(self):
        Worker.run(self)

    def stop(self, timeout=None):
        self._stop_event.set()


class ThreadWorker(threading.Thread, Worker):
    """See `gromozeka.concurrency.Worker` documentation

    Attributes:
        _stop_event(threading.Event): Stop event
    """
    __doc__ = Worker.__doc__

    def __init__(self, pool):
        threading.Thread.__init__(self)
        Worker.__init__(self, pool=pool)
        self._stop_event = threading.Event()

    def run(self):
        Worker.run(self)

    def stop(self, timeout=None):
        self._stop_event.set()
