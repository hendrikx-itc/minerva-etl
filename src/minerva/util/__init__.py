# -*- coding: utf-8 -*-
import datetime
from typing import Optional, Any, Iterable, Callable, List

from time import sleep
from operator import itemgetter, attrgetter, eq
from itertools import repeat, groupby
from functools import partial, reduce


def compose_pair(fn_a, fn_b):
    """
    Return a new function that executes both functions after each other,
    feeding the result of `fn_b` as the argument to `fn_a`.
    """
    def composed(*args, **kwargs):
        return fn_a(fn_b(*args, **kwargs))

    return composed


def compose(*args):
    """
    Return a new function that executes all functions, one after the other,
    passing the output of one to the next as argument.
    """
    return reduce(compose_pair, args)


def apply_fn(f, x):
    """Return the result of f applied to x."""
    return f(x)


def zip_apply(functions):
    """
    Zip the functions with the values and then apply the function to the value.
    """
    def f(values):
        return [fn(v) for fn, v in zip(functions, values)]

    return f


def reorderer(required_order, actual_order):
    """
    Return a function that can reorder a list of values in the same manner that
    `required_order` vs `actual_order` indicates.
    """
    ordered_getters = [itemgetter(actual_order.index(name))
                       for name in required_order]
    count = len(ordered_getters)

    def reorder(values):
        return zip_apply(ordered_getters)(repeat(values, count))

    return reorder


def keys_to_values(mapping, values):
    return [mapping.get(value) for value in values]


def swap(values):
    a, b = values

    return b, a


def attrchecker(name, value):
    """
    Return a function that can check if the attribute of an object has a
    certain value.

    Example:

        >>> from minerva.util import attrchecker
        >>> name_is_bob = attrchecker("name", "bob")

    Calling name_is_bob(obj) returns true if obj.name == "bob" is true.
    """
    get_attr = attrgetter(name)

    return compose(partial(eq, value), get_attr)


def matches(constraints, instance):
    """
    Return a function that checks if all constraints match for a specified
    object. For example:

        >>> from minerva.util import matches
        >>> def gt_10(v):
        ...     return v > 10
        ...
        >>> def lt_20(v):
        ...     return v < 20
        ...
        >>> constraints = [gt_10, lt_20]
        ...
        >>> matches(constraints, 12)
        True
        >>> matches(constraints, 45)
        False
    """
    return all([constraint(instance) for constraint in constraints])


def head(iterable):
    """
    Return the first element of `iterable`
    """
    return iterable[0]


first = head


def tail(iterable):
    """
    Return a list with everything but the first element of `iterable`
    """
    return iterable[1:]


def last(iterable):
    return iterable[-1]


fst = itemgetter(0)


snd = itemgetter(1)


def left(left, _):
    """
    Return the 'left' value of the two arguments.
    """
    return left


def right(_, right):
    """
    Return the 'right' value of the two arguments.
    """
    return right


def each(fn, iterable):
    """
    Apply function `fn` to each item in `iterable`. For example:

        >>> from minerva.util import each
        >>> each(print, ["1 - first", "2 - second", "3 - third"])
        1 - first
        2 - second
        3 - third
    """
    for item in iterable:
        fn(item)


def iter_while(condition, get_item):
    """
    Yield value returned by `get_item` while `condition` returns ``True``.
    """
    while condition():
        yield(get_item())


def iter_queue(stop_event, get_item, empty_exception, empty_timeout=1):
    while not stop_event.is_set():
        try:
            yield(get_item())
        except empty_exception:
            sleep(empty_timeout)


def reraise(exc):
    raise exc


def dict_to_handler(d):
    """
    Create an exception handler function from a dictionary with exception types
    as keys and functions as values.
    """
    def handle(exc):
        handler = d.get(type(exc), partial(reraise, exc))

        return handler()

    return handle


def handle_exceptions(handle, fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            return handle(exc)

    return wrapper


def k(x):
    def fn(*args, **kwargs):
        return x

    return fn


def identity(x):
    return x


def no_op(*args, **kwargs):
    """
    Do nothing
    """
    pass


def after(fn_after, fn):
    """
    Return a function that calls `fn_after` after it calls `fn` without
    altering the result of `fn`.
    """
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)

        fn_after()

        return result

    return wrapper


def retry_while(fn, exception_handlers, condition=k(True), timeout=k(1.0)):
    while condition():
        try:
            return fn()
        except Exception as exc:
            handler = exception_handlers.get(type(exc))

            if handler:
                handler(exc)
            else:
                raise

            sleep(timeout())


def memoize(f, timeout=4):
    _cache = {}
    _max_delta = datetime.timedelta(0, timeout)

    def func(*args, **kwargs):
        now = datetime.datetime.now()
        key = repr(args)

        try:
            result, timestamp = _cache[key]

            if now - timestamp > _max_delta:
                result = f(*args, **kwargs)

                _cache[key] = result, now
        except KeyError:
            result = f(*args, **kwargs)

            _cache[key] = result, now

        return result

    return func


def find(match_fn, iterable):
    """
    Return first item in `iterable` for which function `match_fn` returns
    ``True``. If no items match, ``None`` is returned.
    """
    return next(x for x in iterable if match_fn(x))


def expand_args(fn):
    """
    Return function that wraps `fn` so it accepts a tuple that will be
    expanded as separate arguments. For example:

        >>> from operator import add
        >>> from minerva.util import expand_args
        >>> value_pair = (1, 2)
        >>> expand_args(add)(value_pair)
        3
    """
    def wrapper(args_tuple):
        return fn(*args_tuple)

    return wrapper


def pack_args(fn):
    """
    Return a function that wraps `fn` so it accepts positional arguments that
    will be packed and passed as a tuple.
    """
    def wrapper(*args):
        return fn(args)

    return wrapper


make_tuple = pack_args(tuple)


def if_set(v, f: Callable[[Any], Any]):
    """
    If `v` is set (not None) return the result of f(v). Otherwise return None.
    """
    if v is None:
        return None
    else:
        return f(v)


def lines(sep="\n", lines="") -> List[str]:
    """
    Split string on line separators and return list with lines.
    """
    return lines.split(sep)


def unlines(lines: Iterable[str]) -> str:
    """
    Join lines with a newline character in between.
    """
    return "\n".join(lines)


def show(value: Any):
    """Write value to stdout."""
    print(str(value))


def grouped_by(iterable, key):
    return groupby(sorted(iterable, key=key), key)


def merge_dicts(x: Optional[dict], y: Optional[dict]) -> dict:
    if x is None:
        return y
    elif y is None:
        return x
    else:
        z = x.copy()
        z.update(y)

        return z


def string_fns(fns):
    """
    Return function that calls all functions on the same arguments

    :param fns:
    :return:
    """
    def f(*args, **kwargs):
        for fn in fns:
            fn(*args, **kwargs)

    return f


def flatten(lists):
    """
    :param lists: iterable of lists
    :return: list consisting of all elements from the lists in the iterable
    order
    """
    return [item for sublist in lists for item in sublist]
