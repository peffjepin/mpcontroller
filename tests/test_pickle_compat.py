import pickle
import traceback

import pytest

from .conftest import example_message

import mpcontroller as mpc


def example_exception_and_traceback():
    try:
        n = 1 / 0
    except ZeroDivisionError as exc:
        tb = traceback.format_exc()
        return exc, tb


class ExampleException(mpc.Exception):
    def __init__(self, *args, **kwargs):
        super().__init__("testing")


@pytest.mark.parametrize(
    "object_under_test",
    (
        mpc.UnknownMessageError(example_message, "worker"),
        mpc.WorkerRuntimeError(*example_exception_and_traceback()),
        ExampleException(1, 2),
        ExampleException(kwarg1=1, kwarg2=2),
        ExampleException(1, 2, kwarg1=1, kwarg2=2),
    ),
)
def test_pickle_dump_and_load(object_under_test):
    serialized = pickle.dumps(object_under_test)
    deserialized = pickle.loads(serialized)

    assert object_under_test == deserialized
