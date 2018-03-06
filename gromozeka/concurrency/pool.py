import logging
import multiprocessing
import queue
import threading
import time

from gromozeka.concurrency import commands
from gromozeka.concurrency.worker import ThreadWorker


class Pool(threading.Thread):
    """Pool of workers

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        in_queue(:obj:`multiprocessing.Queue`): Queue to receive input requests
        out_queue(:obj:`multiprocessing.Queue`): Result queue
        cmd_in_queue(:obj:`multiprocessing.Queue`): Command queue
        cmd_out_queue(:obj:`multiprocessing.Queue`): Command result queue
        max_workers(:obj:`int`): Number of workers to start
        worker_class(:class:`gromozeka.concurrency.Worker`): worker class
        workers(:obj:`list` of :obj:`gromozeka.concurrency.Worker`): list of worker instances
        _stop_event(:obj:`multiprocessing.Event`): Stop event

    Args:
        max_workers(:obj:`int`, optional): Number of workers to start
        worker_class(:class:`gromozeka.concurrency.Worker`, optional): Worker class
    """

    __slots__ = ['logger', 'init_max_workers', 'worker_class', 'in_queue', 'out_queue', 'cmd_in_queue', 'cmd_out_queue',
                 '_stop_event', 'workers', 'is_on_stop']

    def __init__(self, max_workers=1, worker_class=ThreadWorker, logger=None):
        self.logger = logger or logging.getLogger("gromozeka.pool")
        self.init_max_workers = max_workers
        self.worker_class = worker_class
        self.in_queue = multiprocessing.Queue(self.init_max_workers)
        self.out_queue = multiprocessing.Queue()
        self.cmd_in_queue = multiprocessing.Queue()
        self.cmd_out_queue = multiprocessing.Queue()
        self._stop_event = multiprocessing.Event()
        self.is_on_stop = False
        self.workers = {}
        super().__init__()

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd_in_queue.get(True, 0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_SHRINK:
                self._shrink(args)
            elif command == commands.POOL_GROW:
                self._grow(args)
            elif command == commands.POOL_REMOVE_WORKER:
                self._remove_worker(args)
            elif command == commands.POOL_SIZE:
                self._size()
            elif command == commands.POOL_STOP:
                self.stop_()

    def start(self):
        """Start pool

        """
        super().start()
        return self.cmd_out_queue.get()

    @property
    def cmd(self):
        """Command queue alias

        Returns:
            multiprocessing.Queue: Command queue
        """
        return self.cmd_in_queue

    def run(self):
        """Start :attr:`workers` in threads or processes

        """
        self.logger.info("start")
        self.grow(self.init_max_workers)
        self.listen_cmd()

    def stop(self):
        """Stop pool and child :attr:`workers`

        """
        self.cmd.put(commands.pool_stop())

    def stop_(self):
        self.is_on_stop = True
        self.out_queue.cancel_join_thread()
        self.in_queue.cancel_join_thread()
        self._shrink(len(self.workers))
        self._stop_event.set()
        self.logger.info("stop")

    def grow(self, n):
        """Grow pool by `n` number of workers

        Args:
            n(int): Number to grow
        """
        self.cmd.put(commands.pool_grow(n))

    def _grow(self, n):
        starting_workers = []
        for _ in range(n):
            worker = self.worker_class(pool=self)
            worker.start()
            self.workers[worker.ident] = worker
            starting_workers.append(worker)
        self._wait_for_start(starting_workers)
        self.in_queue._maxsize = len(self.workers)
        from gromozeka import get_app
        get_app().broker.on_pool_size_changed()

    def shrink(self, n):
        """Shrink pool by `n` number of workers

        Args:
            n(int): Number to grow
        """
        self.cmd.put(commands.pool_shrink(n))

    def _shrink(self, n):
        stopping_workers = []
        for _ in range(n):
            ident = list(self.workers.keys())[-1]
            worker = self.workers.pop(ident)
            worker.stop()
            stopping_workers.append(worker)
        self._wait_for_stop(stopping_workers)
        self.in_queue._maxsize = len(self.workers)
        if self.is_on_stop:
            return
        from gromozeka import get_app
        get_app().broker.on_pool_size_changed()

    def size(self):
        """Get pool size

        Returns:
            int: Number of workers in pool
        """
        self.cmd.put(commands.pool_size())
        return self.cmd_out_queue.get()

    def _size(self):
        self.cmd_out_queue.put(len(self.workers))

    def remove_worker(self, worker_ident):
        """Remove worker from pool by it`s ident

        Args:
            worker_ident: Worker identification
        """
        self.cmd.put(commands.pool_remove_worker(worker_ident))

    def _remove_worker(self, worker_ident):
        del self.workers[worker_ident]
        self.in_queue._maxsize = len(self.workers)
        from gromozeka import get_app
        get_app().broker.on_pool_size_changed()

    def _wait_for_stop(self, workers):
        """Wait until workers stop

        Args:
            workers(:obj:`list` of :obj:`concurrency.Worker`):
        """
        while workers:
            for worker in workers:
                if not worker.is_alive():
                    workers.remove(worker)
                time.sleep(0.05)
        self.cmd_out_queue.put(True)

    def _wait_for_start(self, workers):
        """Wait until workers start

        Args:
            workers(:obj:`list` of :obj:`concurrency.Worker`):
        """
        while workers:
            for worker in workers:
                if worker.is_alive():
                    workers.remove(worker)
                time.sleep(0.05)
        self.cmd_out_queue.put(True)
