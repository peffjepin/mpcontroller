import pytest

from mpcontroller.ipc import Message, Event, Task


@pytest.mark.parametrize("message_type", (Task(), Event()))
def test_ids_are_sequential(message_type):
    prev = 0

    for _ in range(3):
        instance = message_type()
        assert instance.id == prev + 1
        prev = instance.id


@pytest.mark.parametrize(
    "message_type",
    (Task(fields="field1 field2"), Event(fields="field1 field2")),
)
@pytest.mark.parametrize("init_method", ("args", "kwargs"))
def test_initializing_message_with_fields(message_type, init_method):
    if init_method == "args":
        msg = message_type("testing1", "testing2")
    elif init_method == "kwargs":
        msg = message_type(field1="testing1", field2="testing2")

    assert msg.field1 == "testing1"
    assert msg.field2 == "testing2"


@pytest.mark.parametrize(
    "message_type",
    (
        Task(name="ExampleTask", fields="field1 field2"),
        Event(name="ExampleEvent", fields="field1 field2"),
    ),
)
@pytest.mark.parametrize("fmt", (repr, str))
def test_verbose_representation(message_type, fmt):
    msg = message_type("testing1", "testing2")
    actual = fmt(msg)

    assert msg.field1 in actual
    assert msg.field2 in actual
    assert str(msg.id) in actual
    assert msg.__class__.__name__ in actual


@pytest.mark.parametrize(
    "message_type", (Task(fields="value"), Event(fields="value"))
)
def test_only_marked_equal_if_ids_are_the_same(message_type):
    msg1 = message_type(1)
    msg2 = message_type(1)

    assert msg1 == msg1
    assert msg1 != msg2


@pytest.mark.parametrize(
    "message_type, good_match, bad_match",
    (
        (Task(fields="value"), Task, Event),
        (Event(fields="value"), Event, Task),
    ),
)
def test_instance_checking(message_type, good_match, bad_match):
    msg = message_type("testing")

    assert msg.typecode in good_match.typecodes
    assert msg.typecode not in bad_match.typecodes


@pytest.mark.parametrize(
    "message_type",
    (Task(fields="field1 field2"), Event(fields="field1 field2")),
)
@pytest.mark.parametrize("fmt", (tuple, dict))
def test_dumping_and_loading_messages(message_type, fmt):
    msg = message_type("testing1", "testing2")

    dump = Message.dump(msg, fmt)
    reconstructed = Message.load(dump)

    assert msg == reconstructed
    assert type(msg) == type(reconstructed)
    assert reconstructed.id == msg.id
    assert reconstructed.field1 == msg.field1
    assert reconstructed.field2 == msg.field2


@pytest.mark.parametrize(
    "message_type",
    (
        (Task(fields="field1 field2", defaults=("testing1", "testing2"))),
        (Event(fields="field1 field2", defaults=("testing1", "testing2"))),
    ),
)
def test_message_with_default_values(message_type):
    msg = message_type()

    assert msg.field1 == "testing1"
    assert msg.field2 == "testing2"
