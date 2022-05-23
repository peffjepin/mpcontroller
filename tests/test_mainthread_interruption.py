import threading

import pytest

from .conftest import example_exception
from .conftest import exception_soon
from .conftest import happens_soon

from mpcontroller import ipc


@pytest.fixture(autouse=True, scope="module")
def cleanup():
    impl = ipc.MainThreadInterruption.handler
    yield
    ipc.MainThreadInterruption.handler = impl


def raises_example_exception():
    ipc.MainThreadInterruption.interrupt_main(example_exception)


def test_main_thread_interrupted_with_exception_from_child_thread():
    thread = threading.Thread(target=raises_example_exception)

    @exception_soon(example_exception)
    def cause():
        thread.start()


def test_custom_handler_can_be_given(recorded_callback):
    thread = threading.Thread(target=raises_example_exception)
    ipc.MainThreadInterruption.handler = recorded_callback
    thread.start()

    @happens_soon
    def callback_with_exception():
        recorded_callback.assert_called_with(example_exception)
