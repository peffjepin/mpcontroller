import traceback

_TB_NONE = traceback.format_exc()


class PicklableException(Exception):
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj._pickleargs = args
        obj._picklekwargs = kwargs
        return obj

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def __reduce__(self):
        return (self._inflate, (self._pickleargs, self._picklekwargs))

    def __eq__(self, other):
        return (
            isinstance(other, PicklableException)
            and self._pickleargs == other._pickleargs
            and self._picklekwargs == other._picklekwargs
        )

    def __repr__(self):
        fmtargs = ", ".join(map(str, self._pickleargs))
        fmtkwargs = ", ".join(
            f"{k}={v}" for k, v in self._picklekwargs.items()
        )
        sig = fmtargs
        if fmtkwargs:
            if sig:
                sig += f", {fmtkwargs}"
            else:
                sig = fmtkwargs
        return f"{self.__class__.__name__}({sig})"

    @classmethod
    def _inflate(cls, args, kwargs):
        return cls(*args, **kwargs)


class UnknownMessageError(PicklableException):
    def __init__(self, message, recipient):
        self._message = message
        self._recipient = recipient
        self._error = f"{recipient} recieved an unknown message: {message}"
        super().__init__(self._error)


class WorkerExistsError(Exception):
    def __init__(self, worker):
        self.message = f"{worker} already exists"
        super().__init__(self.message)

    def __eq__(self, other):
        return (
            isinstance(other, WorkerExistsError)
            and self.message == other.message
        )


class WorkerRuntimeError(PicklableException):
    def __init__(self, exc, tb=None):
        self.exc = exc
        self.tb = "" if tb == _TB_NONE else tb

        super().__init__()

    def __str__(self):
        return str(self.tb) if self.tb else repr(self.exc)

    def __repr__(self):
        return "WorkerRuntimeError: " + str(self)

    def __eq__(self, other):
        return str(self) == str(other)
