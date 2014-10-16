#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from reader import CEFSocketReader
from writer import CEFSocketWriter
from parser import CEFParser
from webserver import CEFWebServer


from multiprocessing import Process, log_to_stderr, get_logger, Manager
from threading import Thread
import zmq
# import SocketServer

from options import get_config
import os
import re
import time


class ZMQQueue(object):
    def __init__(self):
        self.logger = get_logger()

    def run(self, in_port, out_port):
        in_port = in_port
        out_port = out_port
        try:
            context = zmq.Context(1)
            frontend = context.socket(zmq.PULL)
            frontend.bind('tcp://*:{0}'.format(in_port))


            backend = context.socket(zmq.PUSH)
            backend.setsockopt(zmq.SNDHWM, 1000000)  # roughly 2 gigs * 2 = ~4 GB of caching
            backend.bind('tcp://*:{0}'.format(out_port))

            # http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/devices/streamer.html
            zmq.device(zmq.STREAMER, frontend, backend)

        except Exception as e:
            self.logger.debug('ZMQQueue error')
            self.logger.debug(e)


# when testing on a laptop without a place to send
# class ThreadedEchoRequestHandler(SocketServer.StreamRequestHandler):
#     def handle(self):
#         while True:
#             data = self.rfile.readline()
#         return
#
#
# class ThreadedEchoServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
#     pass


def main():
    # set up logging
    logger = log_to_stderr(logging.DEBUG)  # special multiprocessing.log_to_stderr logger
    logger.handlers[0].formatter._fmt = '%(asctime)s [%(levelname)s/%(processName)s] %(message)s'
    logger.debug('Starting Event Enrichment 2.0')

    # raise SystemExit
    def create_shared_dicts(all_options, subparser_options):
        parser_file_names = set()

        # this thread blocks forever
        while True:
            for option in [all_options, subparser_options]:
                for vendor_product, vendor_product_arguments in option.items():
                    # vendor_product = [mcafee_web_gateway]
                    # vendor_product_arguments = everything underneath [mcafee_web_gateway]
                    for subparser_method, subparser_arguments in vendor_product_arguments.items():
                        # subparser_method = calc_entropy
                        # subparser_arguments = request, cfp1, requestURL.entropy

                        # parse methods have to be named 'parse'
                        if subparser_method == 'parse':
                            for item in subparser_arguments:
                                # convert the comma separated values to a list, stripped out
                                # request, cfp1, requestURL.entropy  ->  ['request', 'cfp1', 'requestURL.entropy']
                                parser_file_names.add([arg.strip() for arg in item.split(',')][0])

                            # now we have a set of file names in parser_file_names.  Let's pull in the patterns.
                            for file in parser_file_names:
                                # TODO: fall back on last known good config
                                # http://stackoverflow.com/questions/5137497/find-current-directory-and-files-directory
                                # get current working directory
                                cwd = os.path.dirname(os.path.realpath(__file__))
                                with open('{0}/parsers/{1}'.format(cwd, file)) as f:
                                    patterns = [line.rstrip('\r\n') for line in f]
                                    try:
                                        # TODO: may want to make a formal loop so I can identify parser that is incorrect
                                        compiled_regex_parsers[file] = [re.compile(p, re.IGNORECASE) for p in patterns]
                                    except:
                                        logger.debug('{} contains an invalid regex parser'.format(file))
                            logger.debug('regex parsers updated')
            time.sleep(60)


    # address = ('localhost', 6070) # let the kernel give us a port
    # server = ThreadedEchoServer(address, ThreadedEchoRequestHandler)
    # t = threading.Thread(target=server.serve_forever)
    # t.setDaemon(True)
    # t.start()

    # multiprocessing shared dict stuff
    manager = Manager()
    compiled_regex_parsers = manager.dict()
    shared_dicts = manager.dict()
    input_output_stats = manager.dict()
    historical_stats = manager.list()


    # input/output hosts from conf
    input_hosts = []
    output_hosts = []
    webserver_host = []
    procs = 4

    # subparsing stuff from conf
    all_options = {}
    subparser_options = {}

    # parse through the conf, setting a few variables - input/output_hosts, all_options, and subparser_options
    for item in get_config():
        if 'default' in item:
            # print 'default =', item
            default_values =  item.get('default')
            for key, value in default_values.items():
                # TODO: probably a better way to do this, not very idiomatic
                if key == 'input':
                    for host in value:
                        position = value.index(host)
                        host = host.split(':')
                        value[position] = (host[0], int(host[1]))
                    input_hosts.extend(value)
                if key == 'output':
                    for host in value:
                        position = value.index(host)
                        host = host.split(':')
                        value[position] = (host[0], int(host[1]))
                    output_hosts.extend(value)
                if key == 'webserver':
                    for host in value:
                        host = host.split(':')
                        webserver_host = (host[0], int(host[1]))
                if key == 'parsers':
                    procs = int(value[0])


        elif 'all' in item:
            all_options = item
        else:
            subparser_options = item

    # start parser refreshing thread
    thread_refresh_parsers = Thread(target=create_shared_dicts, args=[all_options, subparser_options] )
    thread_refresh_parsers.daemon = True
    thread_refresh_parsers.start()

    # start RX queue.  This is a ZMQ Queue Device and will contain raw CEF
    rx_in_port = 5551
    rx_out_port = 5552
    rx_streamer = ZMQQueue()
    Process(name='rx_streamer', target=rx_streamer.run, args=(rx_in_port, rx_out_port)).start()

    # start TX queue.  This is a ZMQ Queue Device and will contain processed events
    tx_in_port = 5553
    tx_out_port = 5554
    tx_streamer = ZMQQueue()
    Process(name='tx_streamer', target=tx_streamer.run, args=(tx_in_port, tx_out_port)).start()

    # start writer procs.  Each host defined in conf gets its own process.
    writer = CEFSocketWriter()
    for i, host in enumerate(output_hosts):
        Process(name='writer{0}'.format(i + 1), target=writer.run, args=(host, input_output_stats)).start()  # forks CEFSocketWriter.run()
        logger.debug('started writer-{0}'.format(i))
        time.sleep(0.1)


    # set up parsers and start them in their own processes.  These parsers do the regex grunt work, entropy, etc
    parser = CEFParser()
    for i in range(procs):
        Process(name='parser{0}'.format(i + 1), target=parser.run, args=(compiled_regex_parsers, shared_dicts, subparser_options, all_options, input_output_stats)).start()  # forks CEFParser.run()
        logger.debug('started parser-{0}'.format(i))
        time.sleep(.1)
        # Process(name='parser{0}'.format(i+1), target=parser.run).start()  # forks CEFParser.run()

    # start reader procs.  Each host defined in conf gets its own process.
    reader = CEFSocketReader()
    for i, host in enumerate(input_hosts):
        Process(name='reader{0}'.format(i + 1), target=reader.run, args=(host, input_output_stats)).start()  # forks CEFSocketReader.run()
        logger.debug('started reader-{0}'.format(i))
        time.sleep(.1)

    webserver = CEFWebServer()
    webserver.run(shared_dicts, input_output_stats, webserver_host, historical_stats)
    # this will block forever, keeing MainProcess open, allowing shared dicts to be accessed by all procs.
    # if MainProcess dies, shared dicts get closed.


if __name__ == '__main__':
    main()
