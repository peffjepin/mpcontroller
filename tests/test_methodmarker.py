from mpcontroller.util import MethodMarker


class ExampleMarked:
    value = 0

    @MethodMarker("test")
    def add1(self):
        self.value += 1


def test_marked_method_called_normally():
    obj = ExampleMarked()

    obj.add1()

    assert obj.value == 1


def test_marked_method_called_from_callback_table():
    obj = ExampleMarked()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 1


def test_subclass_inherits_marks():
    class MarkedBlankSubclass(ExampleMarked):
        pass

    obj = MarkedBlankSubclass()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 1


def test_subclass_overrides_mark():
    class MarkedOverride(ExampleMarked):
        def add1(self):
            self.value += 2

    obj = MarkedOverride()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method()

    assert obj.value == 2


def test_method_with_args():
    class Marked:
        value = 0

        @MethodMarker("test")
        def mark(self, arg1, arg2, kwarg1=0):
            self.value += 1

    obj = Marked()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method(1, 2, kwarg1=3)

    assert obj.value == 1


def test_multiple_marks():
    class Marked:
        m1, m2, m3 = 0, 0, 0

        @MethodMarker("test")
        def mark1(self):
            self.m1 += 1

        @MethodMarker("test")
        def mark2(self):
            self.m2 += 1

        @MethodMarker("hello")
        def mark3(self):
            self.m3 += 1

    obj = Marked()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method()
    for method in MethodMarker.make_callback_table(obj)["hello"]:
        method()

    assert obj.m1 == 1
    assert obj.m2 == 1
    assert obj.m3 == 1


def test_subclass_maintains_its_own_marks():
    class MarkerSubclass(MethodMarker):
        pass

    class Marked:
        v1, v2 = 0, 0

        @MarkerSubclass("test")
        def test1(self):
            self.v1 += 1

        @MethodMarker("test")
        def test2(self):
            self.v2 += 1

    obj = Marked()

    for method in MethodMarker.make_callback_table(obj)["test"]:
        method()
    for method in MarkerSubclass.make_callback_table(obj)["test"]:
        method()

    assert obj.v1 == 1
    assert obj.v2 == 1
