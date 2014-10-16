#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import get_logger, current_process
from threading import Thread
import socket
import time
import zmq

class CEFSocketWriter(object):

    def __init__(self):
        self.logger = get_logger()
        self.outsock = None
        self.try_counter = 0
        self.except_counter = 0
        self.last_count = 0


    def update_input_output_stats(self):
        while True:
            self.input_output_stats[current_process().name] = self.try_counter
            # update the shared dict every second.  Any more isn't necessary.
            time.sleep(1)

    def openConnection(self):
        logger = get_logger()
        while True:
            try:
                self.outsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.outsock.connect((self.host, self.port))
                logger.info('successful connection to {0}:{1}'.format(self.host, self.port))
                break
            except:
                logger.info('Waiting for open socket')
                time.sleep(1)

    def run(self, host, input_output_stats):
        self.logger.debug('CEFSocketWriter.run() is running')
        self.input_output_stats = input_output_stats

        Thread(target=self.update_input_output_stats).start() # create a thread that just handles parsing

        self.context = zmq.Context()
        self.results_receiver = self.context.socket(zmq.PULL)
        self.results_receiver.connect("tcp://127.0.0.1:5554")

        self.host = host[0]
        self.port = host[1]
        self.logger.debug('connecting to {0}:{1}'.format(self.host, self.port))

        while True:  # blocks forever, keeps forked process alive
            # TODO - remove if loop?
            self.cef_event = self.results_receiver.recv_pyobj()
            if self.cef_event:
                try:
                    # print self.cef_event
                    self.outsock.send(self.cef_event)
                    self.try_counter += 1
                except Exception as e:
                    self.openConnection()
                    self.outsock.send(self.cef_event)
                    self.except_counter += 1
