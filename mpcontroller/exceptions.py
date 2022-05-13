class UnknownMessageError(Exception):
    def __init__(self, msg, recipient):
        self._msg = msg
        self._recipient = recipient
        self._error = f"{recipient} recieved an unknown message: {msg}"
        super().__init__(self._error)

    def __reduce__(self):
        return (UnknownMessageError, (self._msg, self._recipient))

    def __eq__(self, other):
        return isinstance(other, UnknownMessageError) and self._error == other._error


class WorkerExistsError(Exception):
    def __init__(self, worker):
        self.message = f"{worker!r} already exists: {worker.pid=}"
        super().__init__(self.message)

    def __eq__(self, other):
        return isinstance(other, WorkerExistsError) and self.message == other.message


class WorkerExitError(Exception):
    def __init__(self, worker_id):
        self.id = worker_id
        super().__init__(f"worker with id {worker_id} died from an error")

    def __eq__(self, other):
        return isinstance(other, WorkerExitError) and self.id == other.id

    def __reduce__(self):
        return (WorkerExitError, (self.id,))
