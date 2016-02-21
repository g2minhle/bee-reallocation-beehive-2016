import httplib
import time
import threading

class TrafficGenerator (threading.Thread):

    def __init__(self, destination_bee, fib_number):
        threading.Thread.__init__(self)
        self.destination_bee = destination_bee
        self.fib_number = fib_number

    def run(self):
        conn = httplib.HTTPConnection("192.168.12.111", 7677)
        start_time = time.time()
        request_url = "/apps/beehive-app/%s/%s" \
                        % (self.destination_bee, self.fib_number)
        conn.request("POST", request_url)
        response = conn.getresponse()
        end_time = time.time()
        total_time = end_time - start_time
        print("--- To bee %s with fibNumber %s for %s seconds ---" \
                % (self.destination_bee, self.fib_number, total_time ))

threads = []

for i in range(0, 5):
    new_thread = TrafficGenerator("bee1", 40)
    new_thread.start()
    threads.append(new_thread)

for i in range(0, 5):
    new_thread = TrafficGenerator("bee2", 40)
    new_thread.start()
    threads.append(new_thread)

for t in threads:
    t.join()
print "Exiting Main Thread"