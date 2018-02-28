"""Module for schedule tasks

"""
import logging
import queue
import sched
import threading

import time
from datetime import datetime, timedelta


class Scheduler(threading.Thread):
    """Scheduler class

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        _stop_event(:obj:`threading.Event`): Stop event
        _shed(:obj:`sched.scheduler`) : Scheduler instance
        _event_queue(:obj:`queue.Queue`): Queue for scheduler events
    """

    __slots__ = ['logger', '_shed', '_event_queue', '_stop_event']

    def __init__(self):
        self.logger = logging.getLogger('gromozeka.scheduler')
        self._shed = sched.scheduler(time.time, time.sleep)
        self._event_queue = queue.Queue()
        self._stop_event = threading.Event()
        super().__init__()

    def run(self):
        """Run scheduler in thread

        """
        self.logger.info("start")
        self.listen_cmd()

    def add(self, wait_time, func, args=None, kwargs=None, priority=1):
        """Add event to scheduler queue

        Args:
            wait_time(float): Delay to run (seconds)
            func: Callable function to run
            args: Func arguments
            kwargs: Func keyword arguments
            priority: Event priority
        """
        self._event_queue.put((wait_time, priority, func, args or [], kwargs or {}))

    def stop(self):
        """Stop scheduler

        """
        self._stop_event.set()

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            self._shed.run(blocking=False)
            try:
                event = self._event_queue.get(True, 0.05)
            except queue.Empty:
                continue
            self._shed.enter(delay=event[0], priority=event[1], action=event[2], argument=event[3], kwargs=event[4])
        self.logger.info("stop")


def delay_from_eta(eta):
    """Make new delay from eta

    Examples:

        .. code-block:: python

            delay_from_eta(countdown='2018-02-20 13:38:33.0') # This event will run 20.02.18 at 13:38:33 once

    Args:
        eta(:obj:`str`): If `exceptions.Retry` occurs other retry will run at this time.
         Other arguments ignored.

    Returns:
        :obj:`int` or :obj:`None`: Delay
    """
    try:
        return time.mktime(time.strptime(eta, "%Y-%m-%d %H:%M:%S.%f")) - time.time()
    except ValueError:
        return time.mktime(time.strptime(eta, "%Y-%m-%d %H:%M:%S")) - time.time()


def new_eta(seconds):
    """Make new eta from summary of current time and `seconds`

    Args:
        seconds(float): Seconds

    Returns:
        str: Datetime in format "%Y-%m-%d %H:%M:%S.%f"
    """
    return (datetime.now() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S.%f")


def delay(last_run=None, every=None, interval=None, at=None):
    """Make new delay from arguments

    Examples:

        .. code-block:: python

            delay(every='days', interval=2) # This event will run every 2 days

            delay(every='weeks', interval=1, at='15:40') # This event will run every week at 15:40

    Args:
        last_run(:obj:`str`, optional): Last run time
        every(:obj:`str`, optional): May be seconds, minutes, hours, days, weeks.
        interval(:obj:`int`, optional): Uses with `every`.
        at(:obj:`str`, optional): Time formatted as `hour:minute` or `hour:minute:second`.

    Returns:
        :obj:`int` or False: new event delay. Returns False when not need more event to run - last run.
    """
    if every:
        if last_run:
            last_run_timestamp = time.mktime(time.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f"))
            interval_seconds = timedelta(**{every: (interval or 1)}).total_seconds()
            next_timestamp = last_run_timestamp + interval_seconds
            next_run_date = datetime.fromtimestamp(next_timestamp)
        else:
            next_run_date = datetime.now()
        if at and every in ('days', 'weeks'):
            time_list = at.split(":")
            hour, minute, second = map(lambda x: int(x),
                                       (time_list[0], time_list[1], time_list[2]) if len(time_list) == 3 else (
                                           time_list[0], time_list[1], 0))
            next_run_date = next_run_date.replace(hour=hour, minute=minute, second=second)
            while next_run_date < datetime.now() or next_run_date.timestamp() < 0:
                next_run_date += timedelta(days=1)
        return (next_run_date - datetime.now()).total_seconds()
