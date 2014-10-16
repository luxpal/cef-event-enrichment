#!/usr/bin/env python
# -*- coding: utf-8 -*-

import SocketServer
from multiprocessing import get_logger, current_process
from threading import Thread
import time
import zmq


class CEFSocketReader(object):
    def __init__(self):
        self.logger = get_logger()
        # self.server.custom_counter = 0
        global global_event_count
        global_event_count = 0


    def update_input_output_stats(self):
        '''
         update a shared dict with statistics
        '''
        while True:
            self.input_output_stats[current_process().name] = global_event_count
            # print self.server
            # update the shared dict every second.  Any more isn't necessary.
            time.sleep(1)

    def run(self, host, input_output_stats):
        self.logger.info('CEFSocketReader.run() is running')
        self.input_output_stats = input_output_stats

        # TODO: figure out how to get global read stats
        Thread(target=self.update_input_output_stats).start()

        self.logger.debug('listening on {0}:{1}'.format(host[0], host[1]))
        self.server = self.TCPServer(host, self.ThreadedTCPRequestHandler)
        # self.server.allow_reuse_address = True
        self.server.serve_forever()  # blocks forever


    class ThreadedTCPRequestHandler(SocketServer.StreamRequestHandler):

        def handle(self):
            self.context = zmq.Context()
            self.zmq_socket = self.context.socket(zmq.PUSH)
            self.zmq_socket.connect('tcp://127.0.0.1:5551')

            self.logger = get_logger()
            self.counter = 0

            self.logger.info('client connected: {0}:{1}'.format(self.client_address[0], self.client_address[1]))
            self.s = time.time()
            while True:
                self.data = self.rfile.readline().strip()
                if self.data:
                    try:
                        self.counter += 1
                        self.zmq_socket.send_pyobj(self.data)
                    except Exception as e:
                        self.logger.debug(e)
                        self.logger.debug(self.data)
                if self.data == '':
                    self.logger.info('client disconnected')
                    break
            self.logger.info('client sent {0} lines in {1}\t{2} EPS'.format(self.counter, time.time() - self.s,
                                                                            (self.counter / (time.time() - self.s))))


    # class TCPServer(SocketServer.ForkingMixIn, SocketServer.TCPServer):  # we already have multiple procs per host, no need to overdo it.
    class TCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
        SocketServer.TCPServer.allow_reuse_address = True
        #ForkingMixIn yields *much* better ingest performance over ThreadingMixIn
        # required to have multiple sockets listen
        pass
