"""Commands to synchronize processes or threads

"""


class Command:
    """Helper class to make commands

    """
    # command number. Find it in `gromozeka.primitives.commands`
    command = None
    # command arguments
    args = None

    def __init__(self, command, args=None):
        self.command = command
        self.args = args

    def as_dict(self):
        return {'command': self.command, 'args': self.args}

    def as_tuple(self):
        return self.command, self.args


POOL_STOP = 1
POOL_GROW = 2
POOL_SHRINK = 3
POOL_SIZE = 4
POOL_REMOVE_WORKER = 5
BROKER_TASK_REGISTER = 6
BROKER_TASK_SEND = 7
BROKER_TASK_SEND_DELAYED = 8
BROKER_TASK_DONE = 9
BROKER_TASK_REJECT = 10
BROKER_ON_POOL_SIZE_CHANGED = 11
BACKEND_RESULT_GET = 12
BACKEND_RESULT_SET = 13
BACKEND_RESULT_DEL = 14
BACKEND_RESULTS_DEL = 15
BACKEND_GRAPH_INIT = 16
BACKEND_GRAPH_GET = 17
BACKEND_GRAPH_UPDATE = 18
BACKEND_GRAPH_RESULT_SET = 19
BACKEND_IS_GROUP_COMPLETED = 20
BACKEND_GROUP_ADD_RESULT = 21
BACKEND_GROUP_GET_RESULT = 22
BACKEND_CHAIN_GET_RESULT = 23


# POOL
def pool_stop():
    """Command to stop pool

    Returns:
        Command:
    """
    return Command(command=POOL_STOP, args=None).as_tuple()


def pool_grow(n):
    """Command to grow pool

    Args:
        n: Number of workers to grow

    Returns:
        Command:
    """
    return Command(command=POOL_GROW, args=n).as_tuple()


def pool_shrink(n):
    """Command to shrink pool

    Args:
        n: Number of workers to shrink

    Returns:
        Command:
    """
    return Command(command=POOL_SHRINK, args=n).as_tuple()


def pool_size():
    """Command to get pool size

    Returns:
        Command:
    """
    return Command(command=POOL_SIZE, args=None).as_tuple()


def pool_remove_worker(worker_ident):
    """Command to remove worker from pool

    Args:
        worker_ident(int): Worker identification

    Returns:
        Command:
    """
    return Command(command=POOL_REMOVE_WORKER, args=worker_ident).as_tuple()


# BROKER
def broker_task_register(task_id, broker_point, options, deserializator):
    """Command to add new consumer with `broker_point` to task

    Args:
        task_id(str): Unique task identification
        broker_point(primitives.BrokerPointType): Broker entry
        options: Specific broker options. See NatsOptions broker for example

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_REGISTER,
                   args={'task_id': task_id, 'broker_point': broker_point, 'options': options,
                         'deserializator': deserializator}).as_tuple()


def broker_task_send(request, broker_point, reply_to=None):
    """Command to publish `primitives.Request` to customer

    Args:
        request: Serialized request
        broker_point(gromozeka.BrokerPoint):
        reply_to(gromozeka.BrokerPoint):

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_SEND,
                   args={'request': request, 'broker_point': broker_point, 'reply_to': reply_to}).as_tuple()


def broker_task_send_delayed(request, broker_point, delay):
    """Command to publish `primitives.Request` to customer

    Args:
        request(primitives.Request): Request to publish
        broker_point:
        delay:

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_SEND_DELAYED,
                   args={'request': request, 'broker_point': broker_point, 'delay': delay}).as_tuple()


def broker_task_done(task_uuid, broker_point, delivery_tag):
    """Command to acknowledge task message by it's broker_adapter point, delivery_tag or scheduler_tag

    Args:
        task_uuid:
        broker_point(primitives.BrokerPointType): Broker entry
        delivery_tag(int): Task delivery tag
    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_DONE,
                   args={'task_uuid': task_uuid, 'broker_point': broker_point, 'delivery_tag': delivery_tag}).as_tuple()


def broker_task_reject(broker_point, delivery_tag):
    """Command to reject task message by it's broker_adapter point, delivery_tag or scheduler_tag

    Args:
        broker_point(primitives.BrokerPointType): Broker entry
        delivery_tag(int): Task delivery tag

    Returns:
        Command:
    """
    return Command(command=BROKER_TASK_REJECT,
                   args={'broker_point': broker_point, 'delivery_tag': delivery_tag}).as_tuple()


def broker_on_pool_size_changed():
    """Command to change prefetch_count

    Returns:
        Command:
    """
    return Command(command=BROKER_ON_POOL_SIZE_CHANGED, args=None).as_tuple()


def backend_result_get(task_uuid, graph_uuid):
    """Command to get result from backend_adapter

    Args:
        task_uuid: Task uuid
        graph_uuid: Graph uuid

    Returns:
        Command:
    """
    return Command(command=BACKEND_RESULT_GET, args={'task_uuid': task_uuid, 'graph_uuid': graph_uuid}).as_tuple()


def backend_result_set(task_uuid, result, graph_uuid):
    """Command to set task result

    Args:
        task_uuid: Task uuid
        result: Result
        graph_uuid: Graph uuid
    Returns:
        Command:
    """
    return Command(command=BACKEND_RESULT_SET,
                   args={'task_uuid': task_uuid, 'result': result, 'graph_uuid': graph_uuid}).as_tuple()


def backend_result_del(task_uuid):
    """Command to delete result

    Args:
        task_uuid: Task uuid

    Returns:
        Command:
    """
    return Command(command=BACKEND_RESULT_DEL, args=task_uuid).as_tuple()


def backend_results_del(*task_uuids):
    """Command to delete results

    Args:
        task_uuids: Task uuids

    Returns:
        Command:
    """
    return Command(command=BACKEND_RESULTS_DEL, args=task_uuids).as_tuple()


def backend_graph_init(graph_dict):
    return Command(command=BACKEND_GRAPH_INIT, args=graph_dict).as_tuple()


def backend_graph_get(graph_uuid):
    return Command(command=BACKEND_GRAPH_GET, args=graph_uuid).as_tuple()


def backend_graph_update(graph_uuid, verticies, graph_state=None, error_task_uuid=None, short_error=None):
    return Command(command=BACKEND_GRAPH_UPDATE,
                   args={'graph_uuid': graph_uuid, 'verticies': verticies, 'graph_state': graph_state,
                         'error_task_uuid': error_task_uuid, 'short_error': short_error}).as_tuple()


def backend_graph_result_set(graph_uuid, task_uuid, result):
    return Command(command=BACKEND_GRAPH_RESULT_SET,
                   args={'graph_uuid': graph_uuid, 'task_uuid': task_uuid, 'result': result}).as_tuple()


def backend_is_group_completed(graph_uuid, group_uuid, expected):
    return Command(command=BACKEND_IS_GROUP_COMPLETED,
                   args={'graph_uuid': graph_uuid, 'group_uuid': group_uuid, 'expected': expected}).as_tuple()


def backend_group_add_result(graph_uuid, group_uuid, task_uuid, result):
    return Command(command=BACKEND_GROUP_ADD_RESULT,
                   args={'graph_uuid': graph_uuid, 'group_uuid': group_uuid, 'task_uuid': task_uuid,
                         'result': result}).as_tuple()


def backend_group_get_result(graph_uuid, group_uuid):
    return Command(command=BACKEND_GROUP_GET_RESULT,
                   args={'graph_uuid': graph_uuid, 'group_uuid': group_uuid}).as_tuple()


def backend_chain_get_result(graph_uuid, chain_uuid):
    return Command(command=BACKEND_CHAIN_GET_RESULT,
                   args={'graph_uuid': graph_uuid, 'chain_uuid': chain_uuid}).as_tuple()
