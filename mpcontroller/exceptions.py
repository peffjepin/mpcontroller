class UnknownMessageError(Exception):
    def __init__(self, message, recipient=None, *, _recipient_repr=None):
        self._message = message
        self._recipient = recipient or _recipient_repr
        self._error = f"{recipient} recieved an unknown message: {message}"
        super().__init__(self._error)

    def __reduce__(self):
        return (
            UnknownMessageError,
            (self._message),
            {"_recipient_repr": self._recipient},
        )

    def __eq__(self, other):
        return (
            isinstance(other, UnknownMessageError)
            and self._error == other._error
        )


class WorkerExistsError(Exception):
    def __init__(self, worker):
        self.message = f"{worker} already exists"
        super().__init__(self.message)

    def __eq__(self, other):
        return (
            isinstance(other, WorkerExistsError)
            and self.message == other.message
        )


class UnhandledWorkerError(Exception):
    def __init__(self, exc, tb):
        self.exc = exc
        self.tb = tb
        super().__init__()

    def __reduce__(self):
        return (UnhandledWorkerError, (self.exc, self.tb))

    def __str__(self):
        return str(self.tb)

    def __repr__(self):
        return str(self)
