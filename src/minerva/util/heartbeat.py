# -*- coding: utf-8 -*-
import threading
import time
import logging


class VitalFunction:
    """
    Base class for vital function checks. Override the check method in the
    child class data_provider
    """
    def __init__(self, name):
        self.name = name

    def check(self):
        """
        Override this function in subclass and return True if the vital
        function is still ok, or False if it failed.
        """
        raise NotImplementedError()


class ThreadFunction(VitalFunction):
    """
    Checks if the provided thread is still alive using is_alive
    """
    def __init__(self, name, thread):
        VitalFunction.__init__(self, name)

        self.thread = thread

    def check(self):
        return self.thread.is_alive()


class Heartbeat(threading.Thread):
    """
    Provides a heartbeat

    :param beat_func: Callable object that provides the beat function and will\
        be called at the specified interval
    :param interval: Interval in seconds at which the beat_func is called
    :param vital_functions: The VitalFunction objects that will be checked
    """
    def __init__(self, beat_func, interval=60.0, vital_functions=tuple()):
        threading.Thread.__init__(self, name="Heartbeat")

        self.daemon = True
        self.interval = interval
        self.vital_functions = vital_functions
        self.beat_func = beat_func
        self._stop = False

    def run(self):
        while not self._stop:
            self.beat_func()

            time.sleep(self.interval)

            for vital_function in self.vital_functions:
                function_ok = vital_function.check()

                if not function_ok:
                    logging.error(
                        "Vital function '{0}' failed, heartbeat "
                        "stopped".format(vital_function.name))
                    self._stop = True

    def stop(self):
        """
        Set the stop flag to make the thread stop at the next iteration.
        """
        self._stop = True
