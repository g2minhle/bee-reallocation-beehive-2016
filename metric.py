import httplib
import time
import threading

from time import sleep
from random import random, seed

FIB_NUMBER = 20
INPUT_FILE_NAME = 'test.txt'

class HiveInfo:
    def __init__(self, id, ip, port, message_generation_pattern):
        self.id =  id
        self.ip =  ip
        self.port =  port
        self.message_generation_pattern = message_generation_pattern

class ExperimentRecord():
    def __init__(self, hive_id, bee_id, start_time, end_time, count):
        self.hive_id = hive_id
        self.bee_id = bee_id
        self.start_time = start_time
        self.end_time = end_time
        self.count = count

class ClientBenchmarker():
    def __init__(self):
        self.benchmarkerLock = threading.Lock()
        self.records = []

    def record_time(self, hive_id, bee_id, start_time, end_time, count):
        self.benchmarkerLock.acquire()
        record = ExperimentRecord(hive_id, bee_id, start_time, end_time, count)
        self.records.append(record)
        self.benchmarkerLock.release()

    def outputMetric(self):
        f = open('experimentResult.csv', 'w')
        #f.write(','.join['FromHive', 'DestinationBee', 'StartTime', 'Duration'])
        for i in range(0,len(self.records)):
            record = self.records[i]
            output = [str(record.start_time), str(record.hive_id), str(record.bee_id), str(record.end_time - record.start_time), str(record.count)]
            f.write(','.join(output))
            f.write('\n')
        f.close()


class MessageSender(threading.Thread):
    def __init__(self, hive_info, destination_bee, benchmarker):
        threading.Thread.__init__(self)
        self.hive_info = hive_info
        self.destination_bee = destination_bee
        self.benchmarker = benchmarker

    def run(self):
        ip = self.hive_info.ip
        port = self.hive_info.port
        conn = httplib.HTTPConnection(ip, port)
        start_time = time.time()
        request_url = "/apps/beehive-app/%s/%s" \
                        % (self.destination_bee, FIB_NUMBER)
        print "%s:%s%s" % (ip, port, request_url)
        conn.request("POST", request_url)
        response = conn.getresponse()
        end_time = time.time()
        total_time = end_time - start_time
        self.benchmarker.record_time(self.hive_info.id,
                                        self.destination_bee,
                                        start_time,
                                        end_time,
                                        response.read())

    def send(self):
        self.start()


class TrafficGenerator():
    def __init__(self, file_name, cycle_count, time_interval, benchmarker):
        self.senders = []
        self.cycle_count = cycle_count
        self.time_interval = time_interval
        self.benchmarker = benchmarker
        self._read_input_file(file_name)

    def _read_input_file(self, file_name):
        f = open(file_name, 'r')
        self.bee_count = int(f.readline())
        self.hive_count = int(f.readline())
        f.readline()
        self.hive_infos = []
        for current_hive in range(0, self.hive_count):
            message_generation_pattern = []
            ip = f.readline().strip()
            port = int(f.readline())
            for current_bee in range(0, self.bee_count):
                message_generation_pattern.append(float(f.readline()))
            self.hive_infos.append(HiveInfo(current_hive, ip, port, message_generation_pattern))
            f.readline()

    def _sendRequestPerHive(self, current_hive):
        hive_info = self.hive_infos[current_hive]
        message_generation_pattern = hive_info.message_generation_pattern
        for current_bee in range(0, self.bee_count):
            if (random() < message_generation_pattern[current_bee]):
                messageSender = MessageSender(hive_info, current_bee, self.benchmarker)
                messageSender.send()
                self.senders.append(messageSender)

    def waitForSendersToFinish(self):
        print "Wait for senders to finish."
        for sender in self.senders:
            sender.join()
        print "All senders are done."

    def runExperiment(self):
        print "Start the experiment."
        for i in range(0, self.cycle_count):
            for current_hive in range(0, self.hive_count):
                self._sendRequestPerHive(current_hive)
            sleep(self.time_interval)



# Get from file
# At 1 point of time
# for every hive. Probability that certain mess sage sending to certain bee
# input
# bee hive
#
# run for 1000 cycle
# collect metrics and output result

if __name__ == "__main__":
    print "Init experiment."
    seed()
    clientBenchmarker = ClientBenchmarker()
    trafficGenerator = TrafficGenerator(INPUT_FILE_NAME, 100, 0.01, clientBenchmarker)
    trafficGenerator.runExperiment()
    trafficGenerator.waitForSendersToFinish()
    clientBenchmarker.outputMetric()
