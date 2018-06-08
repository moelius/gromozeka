import inspect
import json
from collections import deque
from uuid import uuid4

import gromozeka
import gromozeka.primitives.base
from gromozeka.primitives.task_pb2 import Task as ProtoTask, ReplyToBrokerPoint as ProtoReplyTo

GROUP = 'GROUP'  # Group operation for parallel computation
CHAIN = 'CHAIN'  # Chain operation for execute tasks one by one
ACHAIN = 'ACHAIN'  # Argumented chain operation for execute tasks one by one with arguments from previous task


class Graph:
    first_uuid = None
    last_uuid = None

    def __init__(self, *args):
        """

        Args:
            args(:obj:`list of :obj:`Vertex`): list of vertexes to make Graph

        """
        self.verticies = {}
        self.uuid = None
        self.state = gromozeka.primitives.base.RECEIVED
        self.error_task_uuid = None
        self.short_error = None
        if args:
            self.first_uuid = args[0].uuid
            self.last_uuid = args[-1].uuid
        for vertex in args:
            self.verticies[vertex.uuid] = vertex

    @property
    def last(self):
        """

        Returns:
            :obj:`Vertex`: Last :obj:`Vertex` of :obj:`Graph`
        """
        return self.vertex_by_uuid(self.last_uuid)

    @property
    def first(self):
        """

        Returns:
            :obj:`Vertex`: First :obj:`Vertex` of :obj:`Graph`

        """
        return self.verticies[self.first_uuid]

    def __or__(self, other):
        """

        Args:
            other(Vertex or Graph):

        Returns:
            Graph:
        """
        return self._op(other, GROUP)

    def __gt__(self, other):
        """

        Args:
            other(Vertex or Graph):

        Returns:
            Graph:
        """
        return self._op(other, CHAIN)

    def __rshift__(self, other):
        return self._op(other, ACHAIN)

    def _op(self, other, op_name):
        """

        Args:
            other(Vertex or Graph): other graph or vertex
            op_name(str): operation name

        Returns:
            Graph:
        """
        if isinstance(other, Graph):
            if op_name == GROUP and self.last.operation == GROUP:
                operation_vertex = self.last
            else:
                operation_vertex = Vertex(task_id=op_name, op_name=op_name)
                self.last.add_edge(operation_vertex)
            other.last.add_edge(operation_vertex)
            self.verticies[operation_vertex.uuid] = operation_vertex
            self.verticies.update(other.verticies)
            if operation_vertex.operation == ACHAIN:
                [setattr(self.vertex_by_uuid(child), 'args_from', self.last.uuid) for child in other.last.children]
            self.last_uuid = self.uuid = operation_vertex.uuid
        else:
            if op_name == CHAIN:
                self._chain(other)
            elif op_name == ACHAIN:
                self._achain(other)
            elif op_name == GROUP:
                self._group(other)
        return self

    def _chain(self, other):
        """

        Args:
            other(Vertex or Graph): other graph or vertex
        """
        self.verticies[other.uuid] = other
        operation_vertex = Vertex(task_id=CHAIN, op_name=CHAIN)
        self.last.add_edge(operation_vertex)
        other.add_edge(operation_vertex)
        self.verticies[operation_vertex.uuid] = operation_vertex
        self.last_uuid = self.uuid = operation_vertex.uuid

    def _achain(self, other):
        """

        Args:
            other(Vertex or Graph): other graph or vertex
        """
        self.verticies[other.uuid] = other
        operation_vertex = Vertex(task_id=ACHAIN, op_name=ACHAIN)
        self.last.add_edge(operation_vertex)
        other.add_edge(operation_vertex)
        other.args_from = self.last.uuid
        self.verticies[operation_vertex.uuid] = operation_vertex
        self.last_uuid = self.uuid = operation_vertex.uuid

    def _group(self, other):
        """

        Args:
            other(Vertex or Graph): other graph or vertex
        """
        self.verticies[other.uuid] = other
        if self.last.operation == GROUP:
            other.add_edge(self.last)
        else:
            operation_vertex = Vertex(task_id=GROUP, op_name=GROUP)
            self.last.add_edge(operation_vertex)
            other.add_edge(operation_vertex)
            self.verticies[operation_vertex.uuid] = operation_vertex
            self.last_uuid = self.uuid = operation_vertex.uuid

    def vertex_by_uuid(self, uuid):
        """Get :obj:`Vertex` by uuid

        Args:
            uuid(str):

        Returns:
            Vertex:
        """
        return self.verticies[uuid]

    def is_group_part(self, vertex):
        if not vertex.parent:
            return False
        return self.vertex_by_uuid(vertex.parent).operation == GROUP

    def is_chain_part(self, vertex):
        if not vertex.parent:
            return False
        return self.vertex_by_uuid(vertex.parent).operation in (CHAIN, ACHAIN)

    def is_chain_tail(self, vertex):
        if not self.is_chain_part(vertex):
            return False
        return vertex.weight == 0

    def as_dict(self):
        d = {}
        vertex_members = dir(Vertex())
        for gk, gv in self.__dict__.items():
            if gk == 'verticies':
                verticies = {}
                for vk, vv in gv.items():
                    vertex = {}
                    for k in dir(vv):
                        v = getattr(vv, k)
                        if k not in vertex_members or k.startswith('__') or inspect.isroutine(v):
                            continue
                        vertex[k] = v
                    verticies.update({vk: vertex})
                d[gk] = verticies
                continue
            d[gk] = gv
        return d

    @classmethod
    def from_dict(cls, data):
        graph = cls()
        verticies = {}
        for gk, gv in data.items():
            if hasattr(graph, gk):
                setattr(graph, gk, gv)
                continue
            vertex = Vertex()
            for vk, vv in json.loads(gv).items():
                setattr(vertex, vk, vv)
            verticies.update({gk: vertex})
        setattr(graph, 'verticies', verticies)
        return graph

    def apply_async(self, graph_uuid=None):
        app = gromozeka.get_app()
        if graph_uuid:
            self.uuid = graph_uuid
        app.backend_adapter.graph_init(self.as_dict())
        for v in self.next_vertex():
            v.apply(self.uuid)

    def next_vertex(self, from_uuid=None):
        stack = deque([self.vertex_by_uuid(from_uuid) if from_uuid else self.last])
        while stack:
            cur = stack[-1]
            if cur.operation:
                if cur.state == gromozeka.primitives.base.SUCCESS:
                    if cur.parent:
                        stack.appendleft(self.vertex_by_uuid(cur.parent))
                else:
                    children = [self.vertex_by_uuid(ch) for ch in cur.children]
                    if cur.operation == GROUP:
                        for ch in children:
                            if ch.state == gromozeka.primitives.base.SUCCESS:
                                break
                        else:
                            stack.extendleft(children)
                    else:
                        for ch in children:
                            if ch.state != gromozeka.primitives.base.SUCCESS:
                                stack.appendleft(ch)
                                break
            elif cur.state != gromozeka.primitives.base.SUCCESS:
                yield cur
            else:
                if cur.parent:
                    stack.appendleft(self.vertex_by_uuid(cur.parent))
            stack.pop()

    def draw(self):
        """Draw workflow graph

        """

        import networkx as nx
        import matplotlib as mpl
        mpl.use('Qt5Agg')
        from matplotlib import pyplot as plt
        nx_graph = nx.DiGraph()
        node_labels = {}
        for uuid, v in self.verticies.items():
            if v.parent:
                nx_graph.add_edge(v.uuid, v.to, weight=v.weight)
            node_labels[v.uuid] = '{}{}'.format(v.task_id or v.operation,
                                                '(R)' if (v.operation and not v.parent) else '')
            nx_graph.add_node(v.uuid, label=v.task_id or v.operation)
        pos = nx.nx_pydot.graphviz_layout(nx_graph)
        nx.draw(nx_graph, pos=pos, node_size=3000, node_color='lightgreen', labels=node_labels)
        edge_labels = nx.get_edge_attributes(nx_graph, 'weight')
        nx.draw_networkx_edge_labels(nx_graph, pos, edge_labels=edge_labels)
        plt.show()


class Vertex:
    def __init__(self, uuid=None, task_id=None, op_name=None, args=None, kwargs=None, bind=None, reply_to_exchange=None,
                 reply_to_routing_key=None):
        self.uuid = uuid or str(uuid4())
        self.bind = bind or False
        self.parent = None
        self.children = []
        self.state = gromozeka.primitives.base.PENDING
        self.operation = op_name
        self.fr = None
        self.to = None
        self.weight = None
        self.task_id = task_id
        self.args = args
        self.kwargs = kwargs
        self.error = None
        self.exc = None
        self.args_from = None
        self.reply_to_exchange = reply_to_exchange
        self.reply_to_routing_key = reply_to_routing_key

    def as_dict(self):
        d = {}
        for vk, vv in self.__dict__.items():
            d[vk] = vv
        return d

    def apply(self, graph_uuid):
        app = gromozeka.get_app()
        bp = app.get_task(self.task_id).broker_point
        if self.reply_to_exchange and self.reply_to_routing_key:
            reply_to = ProtoReplyTo(exchange=self.reply_to_exchange,
                                    routing_key=self.reply_to_routing_key)
        else:
            reply_to = None
        proto_task = ProtoTask(uuid=self.uuid, task_id=self.task_id, graph_uuid=graph_uuid, reply_to=reply_to)
        if self.args:
            proto_task.args = json.dumps(self.args)
        if self.kwargs:
            proto_task.kwargs = json.dumps(self.kwargs)

        app.broker_adapter.task_send(request=proto_task.SerializeToString(), broker_point=bp)

    def add_edge(self, to):
        """

        Args:
            to(Vertex):
        """
        weight = 0 if (to.children and to.operation in (CHAIN, ACHAIN)) or to.operation == GROUP else 1
        to.children.append(self.uuid)
        self.fr = self.uuid
        self.to = to.uuid
        self.weight = weight
        self.parent = to.uuid

    def __or__(self, other):
        """

        Args:
            other(Vertex or Graph):

        Returns:
            Graph:
        """
        return Graph(self).__or__(other)

    def __gt__(self, other):
        """

        Args:
            other(Vertex or Graph):

        Returns:
            Graph:
        """
        return Graph(self).__gt__(other)

    def __rshift__(self, other):
        """

        Args:
            other(Vertex or Graph)::

        Returns:
            Graph:
        """
        return Graph(self).__rshift__(other)
