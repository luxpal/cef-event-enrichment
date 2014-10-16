__author__ = 'mcqueejg'

from tornado import websocket, web, ioloop
from stats import GetStats
import json
import pprint


class IndexHandler(web.RequestHandler):
    def get(self):
        # self.render("./front_end/jared_index.html")
        self.render("./front_end/epoch_test.html")

class WebSocketHandler(websocket.WebSocketHandler):
    clients = []

    def open(self):
        WebSocketHandler.clients.append(self)
        print 'clients now connected =', len(WebSocketHandler.clients)


    def on_message(self, message):
        print 'Message received {0}.'.format(message)


    def on_close(self):
        WebSocketHandler.clients.remove(self)
        print 'clients now connected =', len(WebSocketHandler.clients)

class MyFileHandler(web.StaticFileHandler):
    def initialize(self, path):
        import os
        self.dirname, self.filename = os.path.split(path)
        super(MyFileHandler, self).initialize(self.dirname)

    def get(self, path=None, include_body=True):
        # Ignore 'path'.
        super(MyFileHandler, self).get(self.filename, include_body)


class GetHistoricalData(web.RequestHandler):
    def initialize(self, history):
        # a way to pass objects into Torando handlers
        self.historical = history


    def get_historical_json(self, data, proc_name, proc_sum):
        line_chart_data = []


        # let's do the easy one first.
        if proc_sum is None:
            # data is a list of dicts

            temp_dict = {}
            for item in data:
                # item is a single element of the list
                for time, dict in item.items():
                    sum = 0
                    list_matches =  [(k, v) for (k, v) in dict.iteritems() if proc_name in k]
                    for (label, eps) in list_matches:
                        value_dict = {'time': time, 'y': eps}
                        if temp_dict.has_key(label):
                            temp_dict.get(label).append(value_dict)
                        else:

                            temp_dict[label] = []
                            temp_dict.get(label).append(value_dict)


            for label, values in temp_dict.items():
                # print label, values
                proc_dict = {'label': label,
                             'values': []}
                for item in values:
                    proc_dict.get('values').append(item)
                line_chart_data.append(proc_dict)

            return line_chart_data



        # this is the hard one, sum per epoch key
        elif proc_sum == True:
            if proc_name == 'all':
                procs_to_sum = ['writer', 'parser', 'reader']
            else:
                procs_to_sum = [proc_name]

            sum_dict = {}
            final_dict = {}
            for item in self.historical:
                for time, value in item.items():

                    sum_dict = {}
                    for proc_name, count in value.items():

                        for proc_to_sum in procs_to_sum:
                            if proc_to_sum not in final_dict:
                                final_dict[proc_to_sum] = []
                            if proc_to_sum in proc_name:

                                sum_so_far = sum_dict.get(proc_to_sum)

                                if sum_so_far:
                                    sum_dict[proc_to_sum] = sum_so_far + count
                                else:
                                    sum_dict[proc_to_sum] = count

                    for proc_name, sum_total in sum_dict.items():
                        get_proc_values = final_dict.get(proc_name)
                        get_proc_values.append((time, sum_total))


            beauty = []
            for key, value in final_dict.items():
                my_dict = {'label': key, 'values': []}
                values = my_dict.get('values')
                for time, count in value:
                    values.append({'y': count, 'time': time})
                beauty.append(my_dict)


            return beauty


    def get(self):
        proc_name = self.get_argument('proc')
        proc_sum = self.get_argument('sum', None)
        if proc_sum == 'true':
            proc_sum = True

        tempList = []
        for item in self.historical:
            tempList.append(item)

        # print proc_name
        # print proc_type

        json_data = self.get_historical_json(tempList, proc_name, proc_sum )
        self.write(json.dumps(json_data))
        self.set_header("Content-Type", "application/json")

class CEFWebServer(object):

    def send_stats(self):
        statsObject = GetStats(self.shared_dicts, self.input_output_stats, self.historical_stats)
        data = statsObject.get_stats()

        for c in WebSocketHandler.clients:
            c.write_message(data)

    def run(self, shared_dicts, input_outpus_stats, webserver_host, historical_stats):
        self.shared_dicts = shared_dicts
        self.input_output_stats = input_outpus_stats
        self.historical_stats = historical_stats

        host = webserver_host[0]
        port = webserver_host[1]

        application = web.Application([
            (r'/', IndexHandler),
            (r'/ws', WebSocketHandler),
            # TODO: work on API
            (r'/api/history', GetHistoricalData, dict(history=historical_stats)),
            (r'/assets/(.*)', web.StaticFileHandler, {'path': '/export/home/sysjgm/pythonProjects/enrichment2.0/front_end/assets/'}),
            (r'/epoch_test\.html', MyFileHandler, {'path': '/export/home/sysjgm/pythonProjects/enrichment2.0/front_end/epoch_test.html'})
            ])

        application.listen(port)

        mainLoop = ioloop.IOLoop.instance()
        interval = 1
        scheduler = ioloop.PeriodicCallback(callback=self.send_stats, callback_time=interval*1000, io_loop=mainLoop)

        # these will block mainthread forever?
        scheduler.start()
        mainLoop.start()
