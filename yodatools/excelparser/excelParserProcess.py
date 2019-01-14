import functools
from multiprocessing import Pool
import multiprocessing
import threading
from threading import Thread
import string
import time

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import IntegrityError, ProgrammingError
from pubsub import pub

from odm2api.models import Base, TimeSeriesResultValues, TimeSeriesResults, Units, setSchema
from odm2api.ODMconnection import dbconnection

from yodatools.excelparser.sessionWorker import SessionWorker

mute_x = multiprocessing.Lock()
print_lock = threading.Lock()


def update_output_text(message):
    """
    Updates the Textctrl output window on the summary page

    :param message:
    :return:
    """
    message += '\n'
    pub.sendMessage('controller.update_output_text', message=message)


def commit_tsrvs(session, tsrvs):
    """
    commits TimeSeriesResultValues to database

    :param session: an instance of `sqlalchemy.orm.Session`
    :param tsrvs: a list of TimeSeriesResultValues
    :return: None
    """

    session.add_all(tsrvs)
    try:
        session.commit()
    except (IntegrityError, ProgrammingError):
        session.rollback()
        for i in xrange(0, len(tsrvs)):
            tsrv = tsrvs[i]
            session.add(tsrv)
            try:
                session.commit()
            except (IntegrityError, ProgrammingError) as e:
                session.rollback()
                mute_x.acquire()
                print(e)
                mute_x.release()
                update_output_text('Error: %s' % e.message)


def p_target(queue, conn, thread_count):  # type: (multiprocessing.JoinableQueue, str) -> None

    session_factory = dbconnection.createConnectionFromString(conn)
    engine = session_factory.engine
    setSchema(engine)
    Base.metadata.create_all(engine)
    scoped_session_ = session_factory.Session  # type: scoped_session

    while True:
        args = queue.get()
        if args:
            # create worker threads
            workers = [None] * thread_count
            tsrvs_split = np.array_split(args, thread_count)
            for i in range(len(tsrvs_split)):
                worker = SessionWorker(scoped_session_, print_lock, mute_x, target=commit_tsrvs, args=tsrvs_split[i].tolist())
                worker.daemon = True
                worker.start()
                workers[i] = worker

            # it's probably best to wait for these threads to finish before moving on...
            for w in workers:
                w.join()

        queue.task_done()


def start_procs(conn, processes=1, threads=1):  # type: (str, int, int) -> multiprocessing.Queue
    """
    Starts background processes and returns a queue

    :param conn: connection string to create database connections for each process
    :param processes: the number of processes to create
    :param threads: the number of threads per process to create
    :return: a queue object used to send work to each process
    """
    q = multiprocessing.JoinableQueue()

    # create processes
    procs = [None] * processes
    for i in range(0, processes):
        p = multiprocessing.Process(target=p_target, args=(q, conn, threads), name=string.letters[i])
        p.daemon = True
        procs[i] = p

    # start each process
    for p in procs:
        p.start()

    return q







