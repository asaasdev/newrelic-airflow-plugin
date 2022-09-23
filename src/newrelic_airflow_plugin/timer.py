from __future__ import absolute_import, division, unicode_literals
import functools

from airflow.utils import timezone


def safe_wraps(wrapper, *args, **kwargs):
    """Safely wraps partial functions."""
    while isinstance(wrapper, functools.partial):
        wrapper = wrapper.func
    return functools.wraps(wrapper, *args, **kwargs)


class Timer(object):
    """A context manager/decorator for statsd.timing()."""

    def __init__(self, client, stat):
        self.client = client
        self.stat = stat
        self._elapsed_time = None
        self._sent = False
        self._start_time = None

    def __call__(self, f):
        """Thread-safe timing function decorator."""
        @safe_wraps(f)
        def _wrapped(*args, **kwargs):
            start_time = timezone.utcnow()
            try:
                return f(*args, **kwargs)
            finally:
                elapsed_time = timezone.utcnow() - start_time
                self.client.timing(self.stat, elapsed_time)
        return _wrapped

    def __enter__(self):
        return self.start()

    def __exit__(self, typ, value, tb):
        self.stop()

    def start(self):
        self._sent = False
        self._start_time = timezone.utcnow()
        return self

    def stop(self, send=True):
        if self._start_time is None:
            raise RuntimeError('Timer has not started.')
        self._elapsed_time = timezone.utcnow() - self._start_time
        if send:
            self.send()
        return self

    def send(self):
        if self._elapsed_time is None:
            raise RuntimeError('No data recorded.')
        if self._sent:
            raise RuntimeError('Already sent data.')
        self._sent = True
        self.client.timing(self.stat, self._elapsed_time)
