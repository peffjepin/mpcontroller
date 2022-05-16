from .conftest import Worker
from .conftest import EqualityException
from .conftest import example_message
from .conftest import example_signal
from .conftest import exception_soon

import mpcontroller as mpc


def test_error_in_setup():
    @exception_soon(ErrorInSetup.EXC)
    def cause():
        ErrorInSetup.spawn()


def test_error_in_main():
    @exception_soon(ErrorInMain.EXC)
    def cause():
        ErrorInMain.spawn()


def test_error_in_message_handler():
    @exception_soon(ErrorInMessageHandler.EXC)
    def cause():
        controller = ErrorInMessageHandler.spawn()
        controller.send_message(example_message)


def test_error_in_signal_handler():
    @exception_soon(ErrorInSignalHandler.EXC)
    def cause():
        controller = ErrorInSignalHandler.spawn()
        controller.send_message(example_signal)


def test_error_in_normal_teardown_sequence():
    @exception_soon(ErrorInTeardown.EXC)
    def cause():
        controller = ErrorInTeardown.spawn()
        controller.join()


def test_error_in_teardown_after_an_exception_already_occured():
    @exception_soon(ErrorInMainAndTeardown.EXC)
    def cause():
        ErrorInMainAndTeardown.spawn()


class ErrorInSetup(Worker):
    EXC = EqualityException("setup")

    def setup(self):
        raise self.EXC


class ErrorInMain(Worker):
    EXC = EqualityException("main")

    def main(self):
        raise self.EXC


class ErrorInMessageHandler(Worker):
    EXC = EqualityException("handler")

    @mpc.message_handler(type(example_message))
    def handler(self, msg):
        raise self.EXC


class ErrorInSignalHandler(Worker):
    EXC = EqualityException("signal")

    @mpc.message_handler(example_signal)
    def handler(self):
        raise self.EXC


class ErrorInTeardown(Worker):
    EXC = EqualityException("handler")

    def teardown(self):
        raise self.EXC


class ErrorInMainAndTeardown(Worker):
    EXC = EqualityException("main")

    def main(self):
        raise self.EXC

    def teardown(self):
        raise EqualityException("shouldn't get this one")
