import traceback

from . import config

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
        return isinstance(other, type(self)) and str(self) == str(other)

    @classmethod
    def _inflate(cls, args, kwargs):
        return cls(*args, **kwargs)


class UnknownMessageError(PicklableException):
    def __init__(self, message, error=None):
        if error:
            self._error = error
        else:
            self._error = (
                f"{config.local_context.name} sent an unknown communication:"
                f"\n{message}"
            )
        self._message = message
        super().__init__(self._error)

    def __reduce__(self):
        # important to reduce with _error included, otherwise
        # config.local_context.name will end up being re-evaluated
        # in the recieving process, causing an incorrect error message
        return (UnknownMessageError, (self._message, self._error))


class WorkerRuntimeError(PicklableException):
    def __init__(self, exc, tb=None):
        if tb is None:
            tb = traceback.format_exc()
            tb = tb if tb != _TB_NONE else ""

        self.exc = exc
        self.tb = tb
        super().__init__()

    def __str__(self):
        return str(self.tb) if self.tb else str(self.exc)

    def __repr__(self):
        return "WorkerRuntimeError:\n" + str(self)

    def __eq__(self, other):
        return str(self) == str(other)
