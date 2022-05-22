import multiprocessing as mp

from .conftest import ExampleSignal
from .conftest import RecordedCallback


def test_is_active_after_activate_call(recorded_callback):
    sig = ExampleSignal([recorded_callback])

    assert not sig.is_active
    sig.activate()

    assert sig.is_active
    assert not recorded_callback.called


def test_handling_an_active_signal(recorded_callback):
    sig = ExampleSignal([recorded_callback])

    sig.activate()
    sig.handle()

    assert recorded_callback.called == 1


def test_handling_an_inactive_signal(recorded_callback):
    sig = ExampleSignal([recorded_callback])

    sig.handle()

    assert recorded_callback.called == 0


def test_signal_handled_for_every_activate_call(recorded_callback):
    sig = ExampleSignal([recorded_callback])

    sig.activate()
    sig.activate()
    sig.handle()

    assert recorded_callback.called == 2
    assert not sig.is_active


def test_signal_can_have_multiple_callbacks():
    cb1 = RecordedCallback()
    cb2 = RecordedCallback()
    sig = ExampleSignal([cb1, cb2])

    sig.activate()
    sig.handle()

    assert cb1.called == 1 and cb2.called == 1
    assert not sig.is_active


def handle(sig):
    sig.handle()


def dummy():
    pass


def test_signal_communication_between_processes():
    sig = ExampleSignal([dummy])
    p = mp.Process(target=handle, args=(sig,))

    sig.activate()
    p.start()
    p.join()

    assert not sig.is_active
