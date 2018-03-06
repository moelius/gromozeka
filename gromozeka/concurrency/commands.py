"""Commands to synchronize processes or threads

"""

from typing import NamedTuple, Any


class Command(NamedTuple):
    """Helper class to make commands

    """
    # command number. Find it in `gromozeka.primitives.commands`
    command: int
    # command arguments
    args: Any


POOL_START = 1
POOL_STOP = 2
POOL_GROW = 3
POOL_SHRINK = 4
POOL_SIZE = 5
POOL_REMOVE_WORKER = 6
BROKER_TASK_REGISTER = 7
BROKER_TASK_SEND = 8
BROKER_TASK_DONE = 9
BROKER_TASK_REJECT = 10
BROKER_ON_POOL_SIZE_CHANGED = 11


# POOL
def pool_stop():
    """Command to stop pool

    Returns:
        Command:
    """
    return Command(command=POOL_STOP, args=None)


def pool_grow(n):
    """Command to grow pool

    Args:
        n: Number of workers to grow

    Returns:
        Command:
    """
    return Command(command=POOL_GROW, args=n)


def pool_shrink(n):
    """Command to shrink pool

    Args:
        n: Number of workers to shrink

    Returns:
        Command:
    """
    return Command(command=POOL_SHRINK, args=n)


def pool_size():
    """Command to get pool size

    Returns:
        Command:
    """
    return Command(command=POOL_SIZE, args=None)


def pool_remove_worker(worker_ident):
    """Command to remove worker from pool

    Args:
        worker_ident(int): Worker identification

    Returns:
        Command:
    """
    return Command(command=POOL_REMOVE_WORKER, args=worker_ident)


# BROKER
def broker_task_register(task_id, broker_point):
    """Command to add new consumer with `broker_point` to task

    Args:
        task_id(str): Unique task identification
        broker_point(primitives.BrokerPointType): Broker entry

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_REGISTER, args=dict(task_id=task_id, broker_point=broker_point))


def broker_task_send(request):
    """Command to publish `primitives.Request` to customer

    Args:
        request(primitives.Request): Request to publish

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_SEND, args=request)


def broker_task_done(broker_point, delivery_tag):
    """Command to acknowledge task message by it's broker point, delivery_tag or scheduler_tag

    Args:
        broker_point(primitives.BrokerPointType): Broker entry
        delivery_tag(int): Task delivery tag
    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_DONE, args={'broker_point': broker_point, 'delivery_tag': delivery_tag})


def broker_task_reject(broker_point, delivery_tag):
    """Command to reject task message by it's broker point, delivery_tag or scheduler_tag

    Args:
        broker_point(primitives.BrokerPointType): Broker entry
        delivery_tag(int): Task delivery tag

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_REJECT, args={'broker_point': broker_point, 'delivery_tag': delivery_tag})


def broker_on_pool_size_changed():
    """Command to change prefetch_count

    Args:
        broker_point(primitives.BrokerPointType): Broker entry
        new_size(int): New prefetch_count size

    Returns:
        Command:
    """
    return Command(command=BROKER_ON_POOL_SIZE_CHANGED,args=None)
