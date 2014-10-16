#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import json
import pprint


class GetStats(object):
    def __init__(self, shared_dicts, input_output_stats, historical_stats):
        self.input_output_stats = input_output_stats
        self.historical_stats = historical_stats

    def get_stats(self):
        datalist = []
        temp_list = []
        historical_dict = {}
        calculate_eps = False
        last_values = None
        interval = 1
        current_time = int(time.time())

        if self.input_output_stats.has_key('last_values'):
            calculate_eps = True
            last_values = dict(self.input_output_stats.pop('last_values'))


        for key, value in self.input_output_stats.items():

            new_dict = {'label': key,
                        'y': value,  # always the current incrementing count, gets overwritten later
                        'time': current_time
                       }

            current_count = new_dict.get('y')

            if calculate_eps:
                last_count = last_values.get(key)
                eps = (current_count - last_count) / interval
                new_dict['y'] = eps

                historical_dict[key] = eps



            datalist.append(new_dict)
            temp_list.append((key, current_count))

        self.input_output_stats['last_values'] = temp_list
        another_temp_dict = {}
        another_temp_dict[current_time] = historical_dict

        list_length = len(self.historical_stats)
        if list_length > 300:
            self.historical_stats.pop(0)
            self.historical_stats.append(another_temp_dict)
        else:
            self.historical_stats.append(another_temp_dict)

        # pprint.pprint(json.dumps(datalist))
        return(json.dumps(datalist))
