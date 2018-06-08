import asyncio
import json
import signal
from functools import wraps

import aioredis
import uvloop
from aioredis import ConnectionClosedError, ReplyError
from redis import ConnectionError
from redlock import RedLock

from gromozeka.backends.base import BackendInterface

REDIS_CONNECTION_EXCEPTIONS = (
    OSError, ConnectionClosedError, ReplyError, aioredis.errors.PipelineError, ConnectionError)

TASK_RESULT_PATTERN = 'gromozeka:task:{task_uuid}'  # hash
GRAPH_META_PATTERN = 'gromozeka:graph:{graph_uuid}'  # hash
GRAPH_LOCK_PATTERN = 'gromozeka:graph:{graph_uuid}:lock'  # redlock
GRAPH_TASK_PATTERN = 'gromozeka:graph:{graph_uuid}:task:{task_uuid}'  # key
GRAPH_TASK_GROUP_RESULT_PATTERN = 'gromozeka:graph:{graph_uuid}:group:{group_uuid}:result'  # list
GRAPH_TASK_GROUP_UUIDS_PATTERN = 'gromozeka:graph:{graph_uuid}:group:{group_uuid}:uuids'  # list
GRAPH_TASK_GROUP_LOCK_PATTERN = 'gromozeka:graph:{graph_uuid}:group:{group_uuid}:lock'  # redlock
GRAPH_TASK_CHAIN_RESULT_PATTERN = 'gromozeka:graph:{graph_uuid}:chain:{chain_uuid}'  # key


def async_error_handler(func):
    """

    Returns:
        function:
    """

    @wraps(func)
    async def wrap(*args, **kwargs):
        self = args[0]
        if 'retries' in kwargs:
            retries = kwargs['retries']
            del (kwargs['retries'])
        else:
            retries = self.reconnect_max_retries
        try:
            return await func(*args, **kwargs)
        except REDIS_CONNECTION_EXCEPTIONS as e:
            if isinstance(e, OSError) and 'Errno 61' not in str(e):
                raise
            if not retries:
                self.app.stop_signal(signal.SIGTERM)
            retries -= 1
            await asyncio.sleep(self.reconnect_retry_countdown)
            self.logger.info('trying to reconnect on error: %s', e)
            await wrap(*args, **kwargs, retries=retries)

    return wrap


class RedisAioredisAdaptee(BackendInterface):
    result_queue = None
    reconnect_max_retries = None
    reconnect_retry_countdown = None
    _connection = None
    _loop = None
    _url = None

    def __init__(self, app):
        """

        Args:
            app(gromozeka.Gromozeka):
        """
        self.app = app
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        super().__init__(app=app)

    def configure(self):
        self._url = self.app.config.backend_url
        self.reconnect_max_retries = self.app.config.backend_reconnect_max_retries
        self.reconnect_retry_countdown = self.app.config.backend_reconnect_retry_countdown

    @staticmethod
    def worker_run(self):
        """

        Args:
            self(gromozeka.backends.base.BackendAdapter):
        """
        self.backend._loop = asyncio.new_event_loop()
        self.backend._loop.create_task(self.backend.serve())
        self.backend._loop.run_forever()

    @async_error_handler
    async def serve(self):
        """Start `Backend`

        """
        self._connection = await aioredis.create_redis_pool(self._url, encoding='utf-8')

    def stop(self):
        if self._connection:
            asyncio.run_coroutine_threadsafe(self._stop(), loop=self._loop).result()
        asyncio.run_coroutine_threadsafe(self._loop.shutdown_asyncgens(), loop=self._loop)
        self._loop.stop()

    def result_get(self, task_uuid, graph_uuid=None):
        self.result_queue.put(
            asyncio.run_coroutine_threadsafe(self._result_get(task_uuid, graph_uuid), loop=self._loop).result())

    def result_set(self, task_uuid, result, graph_uuid=None):
        asyncio.run_coroutine_threadsafe(self._result_set(task_uuid=task_uuid, result=result, graph_uuid=graph_uuid),
                                         loop=self._loop).result()
        self.result_queue.put(True)

    def result_del(self, task_uuid):
        raise NotImplementedError

    def results_del(self, *task_uuids):
        raise NotImplementedError

    def graph_init(self, graph_dict):
        self.result_queue.put(asyncio.run_coroutine_threadsafe(self._graph_init(
            graph_dict=graph_dict), loop=self._loop).result())

    def graph_get(self, graph_uuid):
        r = asyncio.run_coroutine_threadsafe(self._graph_get(graph_uuid=graph_uuid), loop=self._loop).result()
        self.result_queue.put(r)

    def graph_update(self, graph_uuid, verticies, graph_state=None, error_task_uuid=None, short_error=None):
        res = asyncio.run_coroutine_threadsafe(
            self._graph_update(graph_uuid=graph_uuid, verticies=verticies, graph_state=graph_state,
                               error_task_uuid=error_task_uuid, short_error=short_error),
            loop=self._loop).result()
        self.result_queue.put(res)

    def graph_result_set(self, graph_uuid, task_uuid, result):
        res = asyncio.run_coroutine_threadsafe(
            self._graph_result_set(graph_uuid=graph_uuid, task_uuid=task_uuid, result=result),
            loop=self._loop).result()
        self.result_queue.put(res)

    def is_group_completed(self, graph_uuid, group_uuid, expected):
        res = asyncio.run_coroutine_threadsafe(
            self._is_group_completed(graph_uuid=graph_uuid, group_uuid=group_uuid, expected=expected),
            loop=self._loop).result()
        self.result_queue.put(res)

    def group_add_result(self, graph_uuid, group_uuid, task_uuid, result):

        res = asyncio.run_coroutine_threadsafe(
            self._group_add_result(graph_uuid=graph_uuid, group_uuid=group_uuid, task_uuid=task_uuid,
                                   result=result),
            loop=self._loop).result()
        self.result_queue.put(res)

    def group_get_result(self, graph_uuid, group_uuid):
        self.result_queue.put(
            asyncio.run_coroutine_threadsafe(self._group_get_result(graph_uuid=graph_uuid, group_uuid=group_uuid),
                                             loop=self._loop).result())

    def chain_get_result(self, graph_uuid, chain_uuid):
        self.result_queue.put(
            asyncio.run_coroutine_threadsafe(self._chain_get_result(graph_uuid=graph_uuid, chain_uuid=chain_uuid),
                                             loop=self._loop).result())

    @async_error_handler
    async def _stop(self):
        self._connection.close()
        await self._connection.wait_closed()

    @async_error_handler
    async def _result_get(self, task_uuid, graph_uuid=None):
        res = await self._connection.get(
            TASK_RESULT_PATTERN.format(task_uuid=task_uuid) if not graph_uuid else GRAPH_TASK_PATTERN.format(
                graph_uuid=graph_uuid, task_uuid=task_uuid))
        return json.loads(res)

    @async_error_handler
    async def _result_set(self, task_uuid, result, graph_uuid=None):
        return await self._connection.set(
            TASK_RESULT_PATTERN.format(task_uuid=task_uuid) if not graph_uuid else GRAPH_TASK_PATTERN.format(
                graph_uuid=graph_uuid, task_uuid=task_uuid), json.dumps(result))

    @async_error_handler
    async def _result_del(self, task_uuid):
        raise NotImplemented

    @async_error_handler
    async def _results_del(self, *task_uuids):
        raise NotImplemented

    @async_error_handler
    async def _graph_init(self, graph_dict):
        graph_hash = {
            'uuid': graph_dict['uuid'],
            'state': graph_dict['state'],
            'first_uuid': graph_dict['first_uuid'],
            'last_uuid': graph_dict['last_uuid'],
            'error_task_uuid': '',
            'short_error': '',
        }
        for k, v in graph_dict['verticies'].items():
            graph_hash[k] = json.dumps(v)
        await self._connection.hmset_dict(GRAPH_META_PATTERN.format(graph_uuid=graph_dict['uuid']), graph_hash)

    @async_error_handler
    async def _graph_get(self, graph_uuid):
        return await self._connection.hgetall(GRAPH_META_PATTERN.format(graph_uuid=graph_uuid))

    @async_error_handler
    async def _graph_update(self, graph_uuid, verticies, graph_state=None, error_task_uuid=None, short_error=None):
        pipe = self._connection.pipeline()
        graph_ident = GRAPH_META_PATTERN.format(graph_uuid=graph_uuid)
        for vertex in verticies:
            pipe.hset(graph_ident, vertex['uuid'], json.dumps(vertex))
        if graph_state:
            pipe.hset(graph_ident, 'state', graph_state)
            pipe.hset(graph_ident, 'error_task_uuid', error_task_uuid or '')
            pipe.hset(graph_ident, 'short_error', short_error or '')
        await pipe.execute()

    @async_error_handler
    async def _graph_result_set(self, graph_uuid, task_uuid, result):
        await self._connection.set(GRAPH_TASK_PATTERN.format(graph_uuid=graph_uuid, task_uuid=task_uuid),
                                   json.dumps(result))

    @async_error_handler
    async def _is_group_completed(self, graph_uuid, group_uuid, expected):
        return (await self._connection.llen(
            GRAPH_TASK_GROUP_UUIDS_PATTERN.format(graph_uuid=graph_uuid, group_uuid=group_uuid))) >= expected

    @async_error_handler
    async def _group_add_result(self, graph_uuid, group_uuid, task_uuid, result):
        with RedLock('%s_%s' % (graph_uuid, group_uuid), connection_details=[{'url': self._url}]):
            pipe = self._connection.pipeline()
            pipe.rpush(GRAPH_TASK_GROUP_UUIDS_PATTERN.format(graph_uuid=graph_uuid, group_uuid=group_uuid), task_uuid)
            pipe.rpush(GRAPH_TASK_GROUP_RESULT_PATTERN.format(graph_uuid=graph_uuid, group_uuid=group_uuid),
                       json.dumps(result))
            group_len = pipe.llen(GRAPH_TASK_GROUP_UUIDS_PATTERN.format(graph_uuid=graph_uuid, group_uuid=group_uuid))
            await pipe.execute()
            return await group_len

    @async_error_handler
    async def _group_get_result(self, graph_uuid, group_uuid):
        res = await self._connection.lrange(
            GRAPH_TASK_GROUP_RESULT_PATTERN.format(graph_uuid=graph_uuid, group_uuid=group_uuid), 0, -1)
        return [json.loads(r) for r in res]

    @async_error_handler
    async def _chain_get_result(self, graph_uuid, chain_uuid):
        return await self._connection.get(
            GRAPH_TASK_CHAIN_RESULT_PATTERN.format(graph_uuid=graph_uuid, chain_uuid=chain_uuid))
