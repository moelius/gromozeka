import sys
import traceback


class GromozekaException(Exception):
    """Base exception"""
    pass


class Retry(GromozekaException):
    """Exception for retry.

    Attributes:
        max_retries(:obj:`int`): Maximum number of retries
        retry_countdown(:obj:`float`): Pause between retries (seconds)

    Args:
        *args: Exception
        max_retries(int): Maximum number of retries
        retry_countdown(float): Pause between retries (seconds)

    Use this exceptions in task to retry it

    Examples:
        .. code-block:: python

            @task()
            def some_task():
                try:
                    a = 1 / 0
                except ZeroDivisionError as e:
                    raise Retry(e)

        or with custom number of retries and retry_countdown:


        .. code-block:: python

            @task()
            def some_task():
                try:
                    a = 1 / 0
                except ZeroDivisionError as e:
                    raise Retry(e,max_retries=10, retry_countdown=3)
    """

    def __init__(self, *args, max_retries=None, retry_countdown=None):
        _, _, exc_traceback = sys.exc_info()
        self.traceback = traceback.format_exc() if exc_traceback else ""
        self.max_retries = max_retries
        self.retry_countdown = retry_countdown
        super().__init__(*args)


class MaxRetriesExceedException(GromozekaException):
    """Exception will raise if maximum number of retries is reached

    """
    pass


__all__ = ['GromozekaException', 'Retry', 'MaxRetriesExceedException']
