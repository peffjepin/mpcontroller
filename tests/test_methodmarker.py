from mpcontroller.util import MethodMarker


example_marker = MethodMarker()


class ExampleMarked:
    value = 0

    @example_marker.mark("test")
    def add1(self):
        self.value += 1


def test_marked_method_called_normally():
    obj = ExampleMarked()

    obj.add1()

    assert obj.value == 1


def test_marked_method_called_from_callback_table():
    obj = ExampleMarked()

    for method in example_marker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 1


def test_subclass_inherits_marks():
    class MarkedBlankSubclass(ExampleMarked):
        pass

    obj = MarkedBlankSubclass()

    for method in example_marker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 1


def test_subclass_overrides_mark():
    class MarkedOverride(ExampleMarked):
        def add1(self):
            self.value += 2

    obj = MarkedOverride()

    for method in example_marker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 2


def test_method_with_args():
    marker = MethodMarker()

    class Marked:
        value = 0

        @marker.mark("test")
        def mark(self, arg1, arg2, kwarg1=0):
            self.value += 1

    obj = Marked()

    for method in marker.make_callback_table(obj)["test"]:
        method(1, 2, kwarg1=3)

    assert obj.value == 1


def test_multiple_marks():
    marker = MethodMarker()

    class Marked:
        m1, m2, m3 = 0, 0, 0

        @marker.mark("test")
        def mark1(self):
            self.m1 += 1

        @marker.mark("test")
        def mark2(self):
            self.m2 += 1

        @marker.mark("hello")
        def mark3(self):
            self.m3 += 1

    obj = Marked()

    for method in marker.make_callback_table(obj)["test"]:
        method()
    for method in marker.make_callback_table(obj)["hello"]:
        method()

    assert obj.m1 == 1
    assert obj.m2 == 1
    assert obj.m3 == 1


def test_each_instance_maintains_its_own_marks():
    marker1 = MethodMarker()
    marker2 = MethodMarker()

    class Marked:
        v1, v2 = 0, 0

        @marker1.mark("test")
        def test1(self):
            self.v1 += 1

        @marker2.mark("test")
        def test2(self):
            self.v2 += 1

    obj = Marked()

    for method in marker1.make_callback_table(obj)["test"]:
        method()
    for method in marker2.make_callback_table(obj)["test"]:
        method()

    assert obj.v1 == 1
    assert obj.v2 == 1
