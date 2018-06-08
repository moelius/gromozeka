import logging
import multiprocessing
import queue
import threading

import gromozeka.app
from gromozeka.concurrency import commands
from gromozeka.concurrency.worker import ThreadWorker, ProcessWorker


class Pool(threading.Thread):
    """Pool of workers

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        worker_queue(:obj:`multiprocessing.Queue`): Queue to receive input requests
        cmd(:obj:`multiprocessing.Queue`): Command queue
        cmd_out(:obj:`multiprocessing.Queue`): Command result queue
        max_workers(:obj:`int`): Number of workers to start
        worker_class(:class:`gromozeka.concurrency.Worker`): worker class
        workers(:obj:`list` of :obj:`gromozeka.concurrency.Worker`): list of worker instances
        _stop_event(:obj:`multiprocessing.Event`): Stop event

    Args:
        max_workers(:obj:`int`, optional): Number of workers to start
        worker_class(:class:`gromozeka.concurrency.Worker`, optional): Worker class
    """

    __slots__ = ['logger', 'init_max_workers', 'worker_cls', 'worker_queue', 'cmd', 'cmd_out', '_stop_event', 'workers',
                 'is_on_stop']

    def __init__(self, max_workers=1, worker_class=ThreadWorker, logger=None):
        self.logger = logger or logging.getLogger("gromozeka.pool")
        self.init_max_workers = max_workers
        self.worker_cls = worker_class
        self.worker_queue = multiprocessing.Queue() if issubclass(self.worker_cls, ProcessWorker) else queue.Queue()
        from gromozeka.brokers import BrokerAdapter
        from gromozeka.backends import BackendAdapter
        self.cmd = multiprocessing.Queue() if isinstance(self, (BrokerAdapter, BackendAdapter)) else queue.Queue()
        self.cmd_out = multiprocessing.Queue() if isinstance(self, (BrokerAdapter, BackendAdapter)) else queue.Queue()
        self._stop_event = threading.Event()
        self.is_on_stop = False
        self.workers = {}
        self.lock = threading.Lock()
        super().__init__()

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd.get(True, 0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_SHRINK:
                self._shrink(args)
            elif command == commands.POOL_GROW:
                self._grow(args)
            elif command == commands.POOL_REMOVE_WORKER:
                self._remove_worker(args)
            elif command == commands.POOL_STOP:
                self.stop_()

    def start(self):
        """Start pool

        """
        with self.lock:
            super().start()
            self.cmd_out.get()
        gromozeka.app.get_app().broker_adapter.on_pool_size_changed()

    def run(self):
        """Start :attr:`workers` in threads or processes

        """
        self.logger.info("start")
        self._grow(self.init_max_workers)
        self.listen_cmd()

    def stop(self):
        """Stop pool and child :attr:`workers`

        """
        with self.lock:
            self.cmd.put(commands.pool_stop())
            self.cmd_out.get()
        self.logger.info("stop")

    def stop_(self):
        self.is_on_stop = True
        self._shrink(len(self.workers))
        self._stop_event.set()

    def grow(self, n):
        """Grow pool by `n` number of workers

        Args:
            n(int): Number to grow
        """
        with self.lock:
            self.cmd.put(commands.pool_grow(n))
            self.cmd_out.get()
        gromozeka.app.get_app().broker_adapter.on_pool_size_changed()

    def _grow(self, n):
        workers = []
        for _ in range(n):
            worker = self.worker_cls(pool=self)
            worker.start()
            self.workers[worker.ident] = worker
            workers.append(worker)
        while workers:
            for worker in workers:
                if worker.is_alive():
                    workers.remove(worker)
        self.cmd_out.put(True)

    def shrink(self, n):
        """Shrink pool by `n` number of workers

        Args:
            n(int): Number to grow
        """
        with self.lock:
            self.cmd.put(commands.pool_shrink(n))
            self.cmd_out.get()
        gromozeka.app.get_app().broker_adapter.on_pool_size_changed()

    def _shrink(self, n):
        workers = []
        for ident in list(self.workers.keys())[0:n]:
            worker = self.workers.pop(ident)
            worker.stop()
            workers.append(worker)
        while workers:
            for worker in workers:
                if not worker.is_alive():
                    workers.remove(worker)
        self.cmd_out.put(True)

    def remove_worker(self, worker_ident):
        """Remove worker from pool by it`s ident

        Args:
            worker_ident: Worker identification
        """
        # with self.lock:
        self.cmd.put(commands.pool_remove_worker(worker_ident))
        # self.cmd_out.get()
        gromozeka.app.get_app().broker_adapter.on_pool_size_changed()

    def _remove_worker(self, worker_ident):
        del self.workers[worker_ident]
        self.cmd_out.put(True)

    @property
    def size(self):
        """Not command do not use this in workers

        Returns:

        """
        with self.lock:
            return len(self.workers)
