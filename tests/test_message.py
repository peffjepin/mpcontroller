import pytest

from mpcontroller.ipc import (
    Event,
    Task,
    task,
    event,
    dump_message,
    load_message,
)


class BlankEvent(Event):
    pass


class BlankTask(Task):
    pass


class EventWithFields(Event):
    field1: str
    field2: str


class TaskWithFields(Task):
    field1: str
    field2: str


BlankEventFunctional = event("BlankEventFunctional")
BlankTaskFunctional = task("BlankTaskFunctional")
EventWithFieldsFunctional = event("EventWithFieldsFunctional", "field1 field2")
TaskWithFieldsFunctional = task("TaskWithFieldsFunctional", "field1 field2")

blank_message_types = (
    BlankTask,
    BlankTaskFunctional,
    BlankEvent,
    BlankEventFunctional,
)

message_types_with_field1_field2 = (
    EventWithFields,
    EventWithFieldsFunctional,
    TaskWithFields,
    TaskWithFieldsFunctional,
)


@pytest.mark.parametrize("message_type", blank_message_types)
def test_this_test_module_should_be_the_module(message_type):
    class Blank:
        pass

    assert Blank.__module__ == message_type.__module__


@pytest.mark.parametrize("message_type", blank_message_types)
def test_ids_are_sequential(message_type):
    prev = 0

    for _ in range(3):
        instance = message_type()
        assert instance.id == prev + 1
        prev = instance.id


@pytest.mark.parametrize("message_type", message_types_with_field1_field2)
@pytest.mark.parametrize("init_method", ("args", "kwargs"))
def test_initializing_message_with_fields(message_type, init_method):
    if init_method == "args":
        msg = message_type("testing1", "testing2")
    elif init_method == "kwargs":
        msg = message_type(field1="testing1", field2="testing2")

    assert msg.field1 == "testing1"
    assert msg.field2 == "testing2"


@pytest.mark.parametrize("message_type", message_types_with_field1_field2)
@pytest.mark.parametrize("fmt", (repr, str))
def test_verbose_representation(message_type, fmt):
    msg = message_type("testing1", "testing2")
    actual = fmt(msg)

    assert msg.field1 in actual
    assert msg.field2 in actual
    assert str(msg.id) in actual
    assert msg.__class__.__name__ in actual


@pytest.mark.parametrize("message_type", message_types_with_field1_field2)
def test_only_evaluated_equal_if_ids_are_the_same(message_type):
    msg1 = message_type(field1="test1", field2="test2")
    msg2 = message_type(field1="test1", field2="test2")

    assert msg1 == msg1
    assert msg1 != msg2


@pytest.mark.parametrize(
    "message_type, instanceof",
    (
        (BlankTask, Task),
        (BlankTask, BlankTask),
        (BlankTaskFunctional, Task),
        (BlankEvent, Event),
        (BlankEventFunctional, Event),
    ),
)
def test_is_instance(message_type, instanceof):
    msg = message_type()
    assert isinstance(msg, instanceof)


@pytest.mark.parametrize(
    "message_type, instanceof",
    (
        (BlankTask, Event),
        (BlankTask, TaskWithFields),
        (BlankTaskFunctional, Event),
        (BlankEvent, Task),
        (BlankEventFunctional, Task),
    ),
)
def test_is_not_instance(message_type, instanceof):
    msg = message_type()
    assert not isinstance(msg, instanceof)


@pytest.mark.parametrize("message_type", message_types_with_field1_field2)
@pytest.mark.parametrize("fmt", (tuple, dict))
def test_dumping_and_loading_messages(message_type, fmt):
    msg = message_type("testing1", "testing2")

    dump = dump_message(msg, fmt)
    reconstructed = load_message(dump)

    assert msg == reconstructed
    assert type(msg) == type(reconstructed)
    assert reconstructed.id == msg.id
    assert reconstructed.field1 == msg.field1
    assert reconstructed.field2 == msg.field2


class DefaultsEvent(Event):
    field1: str = "testing1"
    field2: str = "testing2"


class DefaultsTask(Task):
    field1: str = "testing1"
    field2: str = "testing2"


@pytest.mark.parametrize(
    "message_type",
    # fmt: off
    (
        DefaultsEvent,
        DefaultsTask,
        (task("ExampleTask", fields="field1 field2", defaults=("testing1", "testing2"))),
        (event("ExampleEvent", fields="field1 field2", defaults=("testing1", "testing2"))),
    ),
    # fmt: on
)
def test_message_with_default_values(message_type):
    msg = message_type()

    assert msg.field1 == "testing1"
    assert msg.field2 == "testing2"
