import asyncio
import multiprocessing
import threading

from gromozeka.brokers.base import BrokerAdapter, get_broker_factory
from gromozeka.brokers.rabbit import RabbitMQPikaAdaptee


class LatexSema:
    def __init__(self, sema_cls, value):
        self.sema = sema_cls(value)
        if isinstance(self.sema, asyncio.Semaphore):
            self.lock = asyncio.Lock()
        elif isinstance(self.sema, threading.Semaphore):
            self.lock = threading.Lock()
        elif isinstance(self.sema, multiprocessing.Semaphore):
            self.lock = multiprocessing.Lock()
        else:
            raise Exception("unknown semaphore class")

    def acquire(self):
        self.sema.acquire()

    def change(self, delta):
        with self.lock:
            if delta > 0:
                for _ in range(delta):
                    self.release()
            elif delta < 0:
                self.sema._value += delta

    def release(self):
        if self.sema._value >= 0:
            self.sema.release()
        else:
            self.sema._value += 1


#
# class AsyncioSemaphore(asyncio.Semaphore):
#     def change(self, delta):
#         if delta > 0:
#             for _ in range(delta):
#                 self.release()
#         elif delta < 0:
#             self._value += delta
#
#     def release(self):
#         if self._value >= 0:
#             super().release()
#         else:
#             self._value += 1


__all__ = ['BrokerAdapter', 'RabbitMQPikaAdaptee', 'get_broker_factory', 'LatexSema']
