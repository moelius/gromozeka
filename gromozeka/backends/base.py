import logging
import queue

from gromozeka.concurrency import Pool, commands
from gromozeka.concurrency import ThreadWorker
from gromozeka.exceptions import GromozekaException


def get_backend_factory(app):
    """

    Args:
        app (gromozeka.app.app.Gromozeka):

    Returns:
        gromozeka.backends.base.BackendInterface:
    """
    if app.config.backend_url.startswith('redis://'):
        from gromozeka.backends import RedisAioredisAdaptee
        backend_adapter = BackendAdapter(RedisAioredisAdaptee(app=app))
    else:
        raise GromozekaException('unknown backend connection url')
    backend_adapter.backend.configure()
    return backend_adapter


class BackendInterface:
    def __init__(self, app):
        """

        Args:
            app(gromozeka.Gromozeka):
        """
        self.logger = logging.getLogger("gromozeka.backend")
        self.app = app

    @staticmethod
    def worker_run(self):
        """This method will be in thread - start to work here

        """
        raise NotImplementedError

    def stop(self):
        """Stop backend_adapter

        """
        raise NotImplementedError

    def configure(self):
        raise NotImplementedError

    def result_get(self, task_uuid, graph_uuid=None):
        raise NotImplementedError

    def result_set(self, task_uuid, result, graph_uuid=None):
        raise NotImplementedError

    def result_del(self, task_uuid):
        raise NotImplementedError

    def results_del(self, *task_uuids):
        raise NotImplementedError

    def graph_init(self, graph_dict):
        raise NotImplementedError

    def graph_get(self, graph_uuid):
        raise NotImplementedError

    def graph_update(self, graph_uuid, verticies, graph_state=None, error_task_uuid=None, short_error=None):
        raise NotImplementedError

    def graph_result_set(self, graph_uuid, task_uuid, result):
        raise NotImplementedError

    def is_group_completed(self, graph_uuid, group_uuid, expected):
        raise NotImplementedError

    def group_add_result(self, graph_uuid, group_uuid, task_uuid, result):
        raise NotImplementedError

    def group_get_result(self, graph_uuid, group_uuid):
        raise NotImplementedError

    def chain_get_result(self, graph_uuid, chain_uuid):
        raise NotImplementedError


class BackendAdapter(Pool):
    def __init__(self, backend, logger=None):
        """

        Args:
            backend (gromozeka.backends.redis.RedisAioredisAdaptee):
            logger (logging.Logger):
        """
        self.logger = logger or logging.getLogger("gromozeka.backend")
        self.backend = backend
        self.worker = type('BackendWorker', (ThreadWorker,),
                           {'app': self.backend.app, 'backend': self.backend, 'run': self.backend.worker_run,
                            'logger': self.logger})
        super().__init__(max_workers=1, worker_class=self.worker, logger=self.logger)
        self.backend.result_queue = self.cmd_out

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd.get(0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_GROW:
                super()._grow(args)
            elif command == commands.POOL_STOP:
                self.backend.stop()
                super().stop_()
            elif command == commands.BACKEND_RESULT_GET:
                task_uuid, graph_uuid = args['task_uuid'], args['graph_uuid']
                self.backend.result_get(task_uuid=task_uuid, graph_uuid=graph_uuid)
            elif command == commands.BACKEND_RESULT_SET:
                task_uuid, result, graph_uuid = args['task_uuid'], args['result'], args['graph_uuid']
                self.backend.result_set(task_uuid=task_uuid, result=result, graph_uuid=graph_uuid)
                self.logger.info('result saved, task_uuid <%s>', task_uuid)
            elif command == commands.BACKEND_RESULT_DEL:
                self.backend.result_del(task_uuid=args['task_uuid'])
            elif command == commands.BACKEND_RESULT_DEL:
                self.backend.results_del(*args)
            elif command == commands.BACKEND_GRAPH_INIT:
                self.backend.graph_init(graph_dict=args)
            elif command == commands.BACKEND_GRAPH_GET:
                self.backend.graph_get(graph_uuid=args)
            elif command == commands.BACKEND_GRAPH_UPDATE:
                graph_uuid, verticies, graph_state = args['graph_uuid'], args['verticies'], args['graph_state']
                error_task_uuid, short_error = args['error_task_uuid'], args['short_error']
                self.backend.graph_update(graph_uuid=graph_uuid, verticies=verticies, graph_state=graph_state,
                                          error_task_uuid=error_task_uuid, short_error=short_error)
            elif command == commands.BACKEND_GRAPH_RESULT_SET:
                graph_uuid, task_uuid, result = args['graph_uuid'], args['task_uuid'], args['result']
                self.backend.graph_result_set(graph_uuid=graph_uuid, task_uuid=task_uuid, result=result)
            elif command == commands.BACKEND_IS_GROUP_COMPLETED:
                graph_uuid, group_uuid, expected = args['graph_uuid'], args['group_uuid'], args['expected']
                self.backend.is_group_completed(graph_uuid=graph_uuid, group_uuid=group_uuid, expected=expected)
            elif command == commands.BACKEND_GROUP_ADD_RESULT:
                graph_uuid, group_uuid = args['graph_uuid'], args['group_uuid']
                task_uuid, result = args['task_uuid'], args['result']
                self.backend.group_add_result(graph_uuid=graph_uuid, group_uuid=group_uuid, task_uuid=task_uuid,
                                              result=result)
            elif command == commands.BACKEND_GROUP_GET_RESULT:
                graph_uuid, group_uuid = args['graph_uuid'], args['group_uuid']
                self.backend.group_get_result(graph_uuid=graph_uuid, group_uuid=group_uuid)
            elif command == commands.BACKEND_CHAIN_GET_RESULT:
                graph_uuid, chain_uuid = args['graph_uuid'], args['chain_uuid']
                self.backend.chain_get_result(graph_uuid=graph_uuid, chain_uuid=chain_uuid)

    def start(self):
        super().start()

    def result_get(self, task_uuid, graph_uuid=None):
        self.cmd.put(commands.backend_result_get(task_uuid=task_uuid, graph_uuid=graph_uuid))
        return self.cmd_out.get()

    def result_set(self, task_uuid, result, graph_uuid=None):
        self.cmd.put(commands.backend_result_set(task_uuid=task_uuid, result=result, graph_uuid=graph_uuid))
        self.cmd_out.get()

    def result_del(self, task_uuid):
        self.cmd.put(commands.backend_result_del(task_uuid=task_uuid))

    def results_del(self, *task_uuids):
        self.cmd.put(commands.backend_results_del(*task_uuids))

    def graph_init(self, graph_dict):
        self.cmd.put(commands.backend_graph_init(graph_dict=graph_dict))
        self.cmd_out.get()

    def graph_get(self, graph_uuid):
        self.cmd.put(commands.backend_graph_get(graph_uuid=graph_uuid))
        return self.cmd_out.get()

    def graph_update(self, graph_uuid, verticies, graph_state=None, error_task_uuid=None, short_error=None):
        self.cmd.put(commands.backend_graph_update(graph_uuid, verticies, graph_state, error_task_uuid, short_error))
        return self.cmd_out.get()

    def graph_result_set(self, graph_uuid, task_uuid, result):
        self.cmd.put(commands.backend_graph_result_set(graph_uuid=graph_uuid, task_uuid=task_uuid, result=result))
        return self.cmd_out.get()

    def is_group_completed(self, graph_uuid, group_uuid, expected):
        self.cmd.put(
            commands.backend_is_group_completed(graph_uuid=graph_uuid, group_uuid=group_uuid, expected=expected))
        return self.cmd_out.get()

    def group_add_result(self, graph_uuid, group_uuid, task_uuid, result):
        self.cmd.put(
            commands.backend_group_add_result(graph_uuid=graph_uuid, group_uuid=group_uuid, task_uuid=task_uuid,
                                              result=result))
        return self.cmd_out.get()

    def group_get_result(self, graph_uuid, group_uuid):
        self.cmd.put(commands.backend_group_get_result(graph_uuid=graph_uuid, group_uuid=group_uuid))
        return self.cmd_out.get()

    def chain_get_result(self, graph_uuid, chain_uuid):
        self.cmd.put(commands.backend_chain_get_result(graph_uuid=graph_uuid, chain_uuid=chain_uuid))
        return self.cmd_out.get()
