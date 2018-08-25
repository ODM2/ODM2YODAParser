from threading import Thread
from sqlalchemy.orm import scoped_session


class SessionWorker(Thread):
    """
    This threading class is used to make background calls to the
    database while the excel parser is reading an excel file.
    """

    def __init__(self, scoped_session, lock=None, mute_x=None, *args, **kwargs):  # type: (scoped_session, threading.Lock, multiprocessing.Lock, any, any) -> None
        """
        :param scoped_session: a **callable** `sqlalchemy.orm.scoped_session` object
        :param mute_x: an instance of `multiprocessing.Lock`
        :param lock: an instance of `threading.Lock`
        :param args: extra arguments you might include with the call to `super.__init__`
        :param kwargs: extra keyword arguments you might include with call to `super.__init__`
            NOTE: Provide at a minimum the `target=` and `args=` keywords
        """
        Thread.__init__(self, *args, **kwargs)
        self.mute_x = mute_x
        self.print_lock = lock
        self.Session = scoped_session

    def run(self):
        """
        Do not invoke this method by itself. Call `self.start()` instead.

        :var target: a callable

        :var args: For the special purpose of this class, args should be
            a list of TimeSeriesResultValues... at least that's what it
            was created for.
        """
        try:
            target = getattr(self, '_Thread__target', None)
            args = getattr(self, '_Thread__args', None)
            if all([target, args]):
                    target(self.Session(), args)
        except Exception as e:
            self.mute_x.acquire()
            with self.print_lock:
                print(e)
                self.mute_x.release()
        finally:
            self.Session.remove()