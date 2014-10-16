#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import threading
import urllib
from multiprocessing import get_logger, current_process
from threading import Thread
import zmq
import sub_parsers
import re





class CEFParser(object):
    def __init__(self):
        self.logger = get_logger()
        self.counter_valid = 0
        self.counter_invalid = 0
        self.not_allowed_keys = ['\\', '_cefVer']


    def update_input_output_stats(self):
        '''
         update a shared dict with statistics
        '''
        while True:
            # TODO: not make this self
            self.input_output_stats[current_process().name] = (self.counter_valid)
            # update the shared dict every second.  Any more isn't necessary.
            time.sleep(1)


    def assemble_cef_message(self):
        try:
            self.cef_string = '{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7} \n'.format(self.cef_dict.pop('cef_version'),
                                                    self.cef_dict.pop('device_vendor'),
                                                    self.cef_dict.pop('device_product'),
                                                    self.cef_dict.pop('device_version'),
                                                    self.cef_dict.pop('device_event_class_id'),
                                                    self.cef_dict.pop('name'),
                                                    self.cef_dict.pop('severity'),
                                                    self.cef_dict.pop('extension'))
            # and I added {7} above
            # for key, val in self.cef_dict.items():
            #     self.cef_string += '%s=%s ' % (key, val)

            # self.cef_string = '{0}\n'.format(self.cef_string)
            # self.cef_string = '{0}\n'.format(urllib.quote(self.cef_string))
            # print self.cef_string

        except Exception as e:
            self.logger.debug('parser.assemble_cef_message')
            self.logger.debug(e)

    def break_apart_cef(self):
        regex_header = re.compile(
            r'(?P<cef_version>.*?)\|'
            r'(?P<device_vendor>.*?)\|'
            r'(?P<device_product>.*?)\|'
            r'(?P<device_version>.*?)\|'
            r'(?P<device_event_class_id>.*?)\|'
            r'(?P<name>.*?)\|'
            r'(?P<severity>.*?)\|'
            r'(?P<extension>[^$]+)')
        # regex_extension = re.compile(r"\b(?<=\s)([^\s=]+)=(.+?)(?=(?:\s[^\s=]+=|$))")

        if self.event.startswith('CEF:0'):
            try:
                header = regex_header.match(self.event).groupdict()  # regex parse the header portion
                # extension = dict(regex_extension.findall(header['extension']))  # regex parse the extension **CPU INTENSE!**
                # header.pop('extension', None)  # we no longer need header[extension] so remove it
                # self.cef_dict = dict(header, **extension)  # combine both dicts into one.  Found on stackoverflow
                self.cef_dict = dict(header)
                #
                # for item in self.cef_dict.keys():
                #     for key in self.not_allowed_keys:
                #         if key in list(set(item)): #convert to set, then to list - don't want to pop keys that aren't there
                #             self.cef_dict.pop(item)

                return True
            except Exception as e:
                self.logger.debug('break_apart_cef error - invalid CEF syntax')
                self.counter_invalid += 1
                print self.event
                return False
        else:
            self.logger.debug('break_apart_cef error - received non-CEF event')
            self.counter_invalid += 1
            return False

    def send_event(self):
        try:
            # from pprint import pprint
            # pprint(self.cef_dict)
            self.consumer_sender.send_pyobj(self.cef_string)  # where the dict actually gets sent to zmq tx queue
            self.counter_valid += 1
        except Exception as e:
            self.logger.debug('processed_queue.put error')
            self.logger.debug(e)

    def connect_zmq_queue(self):
        try:
            # connect to zmq rx queue device
            self.context = zmq.Context()
            self.consumer_receiver = self.context.socket(zmq.PULL)
            self.consumer_receiver.connect("tcp://127.0.0.1:5552")
            self.logger.debug('consumer_receiver = {0}'.format(self.consumer_receiver))
        except Exception as e:
            self.logger.info('connect_zmq_queue error - cannot connect to RX queue')
            self.logger.debug(e)

        try:
            # connect to zmq tx queue
            self.consumer_sender = self.context.socket(zmq.PUSH)
            self.consumer_sender.connect("tcp://127.0.0.1:5553")
            self.logger.debug('consumer_sender = {0}'.format(self.consumer_sender))
        except Exception as e:
            self.logger.debug('connect_zmq_queue error - cannot connect to TX queue')
            self.logger.debug(e)


    def parse_event(self, config_options, vendor_product):
        try:
            # vendor_product returns the lowercase concat of vendor and product - 'mcafee_web_gateway'
            parser = sub_parsers.SubParsers(self.cef_dict, self.compiled_regex_parsers, self.shared_dicts)  # create a parser object for every event

            # changes McAfee Web Gateway to mcafee_web_gateway

            method_mapping = {'decode_url': parser.decode_url,
                              'calc_entropy': parser.calculate_entropy,
                              'extract_tld': parser.extract_tld,
                              'parse': parser.regex_parse,
                              'last_seen': parser.update_last_seen,
                              'remove_ad_fields': parser.remove_ad_fields,
                              'set_field': parser.set_field,
                              'set_field_if_empty': parser.set_field_if_empty}

            # if the vendor_product is in the config, execute the enrichment in order.
            if vendor_product in config_options:
                # grab a list of methods defined in config
                methods = config_options.get(vendor_product)
                # iterate through the methods like parse, calc_entropy, etc...
                for method_name in methods:
                    # check to make sure the config file's methods are valid
                    if method_name in method_mapping:
                        for method_args in methods.get(method_name):
                            # the conig file is correct, a corresponding method exists in subparsers.py
                            # split the values on commas and remove whitespace
                            # if there is a call to a parser
                            method_mapping[method_name]([arg.strip() for arg in method_args.split(',')])
                    else:
                        self.logger.debug('method {0} not found in class SubParsers'.format(method_name))

        except Exception as e:
            self.logger.debug('parse_event_error')
            self.logger.debug(e)

    # def run(self, unique_src_addresses):
    def run(self, compiled_regex_parsers, shared_dicts, subparser_options, all_options, input_output_stats):
        # compiled_regex_parsers, subparser_options, input_output_stats

        self.logger.debug('CEFParser.run() is running')

        self.compiled_regex_parsers = compiled_regex_parsers
        self.shared_dicts = shared_dicts
        self.input_output_stats = input_output_stats

        Thread(target=self.update_input_output_stats).start() # create a thread that just handles stats
        self.connect_zmq_queue()

        # start the forever-blocking processing chain
        while True:  # blocks forever
            try:
                self.event = self.consumer_receiver.recv_pyobj()  # pull an event out of the zmq Queue

                self.valid_event = self.break_apart_cef()  # either True or False
                if self.valid_event:
                    # we now have a cef_dict to parse however we wish!

                    # get vendor_product, example mcafee_web_gateway
                    vendor_product = '_'.join([self.cef_dict['device_vendor'], self.cef_dict['device_product']]).replace(' ', '_').lower()

                    self.parse_event(subparser_options, vendor_product)
                    # if there is an [all] section, run through the list
                    if all_options:
                        self.parse_event(all_options, 'all')  # for the [all] section in config file
                    self.assemble_cef_message()
                    self.send_event()
            except Exception as e:
                self.logger.debug('run error')
                self.logger.debug(e)
