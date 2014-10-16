#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import get_logger
import math
from tldextract import extract
import re
import urllib
import os


class SubParsers(object):
    def __init__(self, cef_dict, compiled_regex_parsers, shared_dicts):
        self.cef_dict = cef_dict
        self.compiled_regex_parsers = compiled_regex_parsers
        self.shared_dicts = shared_dicts

        self.logger = get_logger()

    def get_cef_value(self, field):
        try:
            #TODO: make it so that you parse the first occurance.  messes up with rt=
            result = re.match(r'.*?{0}\=(.*?)\s\w+\='.format(field), self.cef_dict.get('extension'))
            if result:
                return result.group(1)
            else:
                return None
        except Exception as e:
            self.logger.debug('get_cef_value error')
            self.logger.debug(e)

    def update_cef_value(self, field, value):
        # receives CEF field and a value.  Updates extension if exist, adds if doesn't exist
        try:
            old_value = self.get_cef_value(field)
            # old_value = None if it doesn't already exist
            if old_value:
                # if it exists, do not touch it.
                self.cef_dict['extension'] = self.cef_dict.get('extension').replace('{0}={1}'.format(field, old_value), '{0}={1}'.format(field, value))
            else:
                # does not exist, tack it onto the end
                self.add_cef_value(field, value)
        except Exception as e:
            self.logger.debug('update_cef_value error')
            self.logger.debug(e)


    def set_field_if_empty(self, args):
        field = args[0]
        value = args[1]
        try:
            old_value = self.get_cef_value(field)
            if old_value:
                pass
            else:
                self.add_cef_value(field, value)
        except Exception as e:
            self.logger.debug('set_field_if_empty error')
            self.logger.debug(e)


    def add_cef_value(self, field, value):
        try:
            old_value = self.cef_dict.get('extension')
            new_value = '{0} {1}={2}'.format(old_value, field, value)
            self.cef_dict['extension'] = new_value
        except Exception as e:
            self.logger.debug('add_cef_value error')
            self.logger.debug(e)


    def decode_url(self, args):
        use_field = args[0]
        try:
            string = self.get_cef_value(use_field)
            if string:
                string = urllib.unquote_plus(string)
                self.update_cef_value(use_field, string)
        except Exception as e:
            self.logger.debug('sub_parsers.decode_url error')
            self.logger.debug(e)


    def calculate_entropy(self, args):
        # self.logger.debug('calculating entropy for {0}'.format(args))
        use_fields = [arg.strip() for arg in args[0].split(' ')]
        cef_field = args[1]
        cef_label = args[2]
        try:
            field_values = [self.get_cef_value(field) for field in use_fields if self.get_cef_value(field) is not '' and self.get_cef_value(field) is not None]
            if field_values:
                string = ''.join(field_values)  # not using a period to join because you can pass any two fields
                # print 'cef field = {0},\tvalue = {1}'.format(cef_field, string)

                prob = [ float(string.count(c)) / len(string) for c in dict.fromkeys(list(string)) ]
                entropy = - sum([ p * math.log(p) / math.log(2.0) for p in prob ])
                # TODO: fix the empty string output
                # self.cef_dict[cef_field] = entropy
                # self.cef_dict['{0}Label'.format(cef_field)] = cef_label
                self.add_cef_value(cef_field, entropy)
                self.add_cef_value('{0}Label'.format(cef_field), cef_label)
        except Exception as e:
            self.logger.debug('sub_parsers.calculate_entropy error')
            self.logger.debug(e)


    def extract_tld(self, args):
        # self.logger.debug('extracting TLD for {0}'.format(args))
        use_field = args[0]
        cef_subdomain = args[1]
        cef_subdomain_length = args[2]
        cef_domain = args[3]
        cef_suffix = args[4]

        try:
            url = self.get_cef_value(use_field)
            if url:
                # http://forums.bbc.co.uk/
                extract_result = extract(url)
                result_subdomain = extract_result.subdomain  # forums
                result_len_subdomain = len(extract_result.subdomain)  # 7
                result_domain = extract_result.domain  # bbc
                result_suffix = extract_result.suffix  # co.uk

                # don't store None or zero values
                if result_subdomain:
                    self.add_cef_value(cef_subdomain, extract_result.subdomain)
                if result_len_subdomain > 0:
                    self.add_cef_value(cef_subdomain_length, len(extract_result.subdomain))
                if result_domain:
                    self.add_cef_value(cef_domain, extract_result.domain)
                if result_suffix:
                    self.add_cef_value(cef_suffix, extract_result.suffix)
        except Exception as e:
            self.logger.debug('sub_parsers.extract_tld error')
            self.logger.debug(e)


    def regex_parse(self, args):
        # self.logger.debug('parsing out {0}'.format(args))
        use_file = args[0]
        use_field = args[1]
        cef_field = args[2]

        try:
            if use_file in self.compiled_regex_parsers:
                # self.parser_dict has a key equal to 'use_file'
                string_to_parse = self.get_cef_value(use_field)
                if string_to_parse:
                    regex_matches = []
                    concat_string = ''  # we need an empty concat string to build on
                    # unquoting the string to get cleaner results TODO: may want to do this at the end
                    string_to_parse = urllib.unquote_plus(string_to_parse)
                    # evaluate the URL string against all compiled_patterns.  expensive but doable
                    for regex in self.compiled_regex_parsers[use_file]:
                        if regex.match(string_to_parse):
                            # matches from a single pattern, could be many: [('latlong', '38.897676,-77.03653'), ('address', 'The White House'), ('zoom', '17)]
                            matches = regex.match(string_to_parse).groupdict().items()
                            for match in matches:
                                # add the tuple to a list called hits.  Hits will later be concatenated together
                                regex_matches.append(match)
                    # hits = [('latlong', '38.897676,-77.03653'), ('address', 'The White House'), ('zoom', '17), ('search', 'white house'), ('email', 'blah@blah.com')]
                    if regex_matches:
                        for key, value in regex_matches:
                            if value != '':  # sometimes the values are blank.
                                concat_string += '{0}={1}|'.format(key, value)
                        self.add_cef_value(cef_field, concat_string.strip()[:-1])

        except Exception as e:
            self.logger.debug('sub_parsers.regex_parse error')
            self.logger.debug(e)

    # def update_last_seen(self, last_seen_dict, use_field, cef_field):
    def update_last_seen(self, args):
        # self.logger.debug('updating last seen with {0}'.format(args))
        last_seen_name = args[0]
        use_fields = [arg.strip() for arg in args[1].split(' ')]
        cef_field = args[2]

        try:
            field_values = [self.get_cef_value(field) for field in use_fields if self.get_cef_value(field) is not '' and self.get_cef_value(field) is not None]
            if field_values:
                # index_name = "domain_suffix_last_seen['google', 'co.uk']"
                index_name = '{0}{1}'.format(last_seen_name, field_values)
                # on rare occasions, request is null, which means the string will be ''
                event_time = int(self.get_cef_value('rt')[:-3])  # :-3 takes off the last three zeros
                latest_known_last_seen_time = self.shared_dicts.get(index_name)

                if latest_known_last_seen_time:
                    # if the event is newer than what's in the shared dict:
                    if event_time > latest_known_last_seen_time:
                        # update the shared dict
                        self.shared_dicts[index_name] = event_time
                        self.add_cef_value(cef_field, str(event_time - latest_known_last_seen_time))  # TODO - i don't think we need to include the str() for trailing L for the int.
                    else:
                        self.add_cef_value(cef_field, str(0))
                else:
                    self.shared_dicts[index_name] = event_time
                    self.add_cef_value(cef_field, '')  #this makes the cef_field NULL, which means never been seen before.

        except Exception as e:
            self.logger.debug('sub_parsers.update_last_seen error')
            self.logger.debug(e)
            self.logger.debug(self.cef_dict.get('extension'))

    def set_field(self, args):
        # self.logger.debug('setting field {0}, {1}'.format(args[0], args[1]))
        cef_field = args[0]
        string_field = args[1]
        self.update_cef_value(cef_field, string_field)

    #TODO: fix remove_ad_fields
    def remove_ad_fields(self, args):
        string_field = args[0]

        ad_fields = ['ad.']

        if string_field.lower() == 'true':
            for item in self.cef_dict.keys():
                for key in ad_fields:
                    if key in list(set(item)): #convert to set, then to list - don't want to pop keys that aren't there
                        self.cef_dict.pop(item)


    #TODO - finish this method
    def concat_ad_fields(self, args):
        pass
