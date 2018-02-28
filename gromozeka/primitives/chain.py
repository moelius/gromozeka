"""Chained tasks

Notes:
    not ready yet

"""
import uuid


class Chain(list):
    def __init__(self):
        self.uuid = uuid.uuid4()
        super().__init__()

    def __or__(self, other):
        """

        Args:
            other(Task):

        Returns:

        """
        [task.chain.append(other) for task in self]
        other.parent = self[-1].uuid
        self.append(other)
        return self

    def __call__(self, *args, **kwargs):
        pass
