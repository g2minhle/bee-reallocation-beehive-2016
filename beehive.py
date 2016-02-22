#!/usr/bin/python
"""
A script to create a mininet topology with a given number of hosts
connected by one switch, and run a beehive cluster on them.

Put this script in your ~/mininet folder or give this sciprt the path
to mininet folder(otherwise it won't be able to import from the
mininet Python API). Run it as follows:

sudo -E python beehive.py
                        [number-of-hosts]
                        [path/to/application.go]
                        [path/to/mininet/folder]

THE -E FLAG IS IMPORTANT. If it is not set, your environment variables
will not be preserved while running mininet, so the hosts won't be able
to find your GOPATH.

So far the only way to actually see output from a hive is to run something
like "h1 echo $GOPATH" in the mininet CLI which will echo the GOPATH as well
as whatever else has been printed in h1's console.

You can edit GO_RUN_ARGS to include any arguments you want to pass to
your application EXCEPT -addr, -paddrs, and -statepath.
"""

import os
import sys

from time import sleep

from mininet.cli import CLI
from mininet.node import CPULimitedHost
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.log import setLogLevel

CONTROLLER_HOST_NAME = "hcon"
DEFAULT_PORT = 7677
GO_RUN_ARGS = "-instrument"

class TestTopo(Topo):
    """n hosts connected by a single switch."""

    def build(self, n=2):

        # There will be n beehive hosts with 1 controller host
        # CPU limit of a host
        cpu_imit = .5 / (n + 1)
        switch = self.addSwitch("s1")

        # Rhe controller host will get 50% CPU
        host = self.addHost(CONTROLLER_HOST_NAME, cpu=.5 )
        self.addLink(host, switch)

        for h in range(0, n):
            host = self.addHost("h{}".format(h), cpu=cpu_imit)
            self.addLink(host, switch)

def waitForHive(host_index):
    print("Waiting hive {} hive to start ...".format(host_index))
    path_to_hive_output = "beehiveOutput/{}.out".format(host_index)
    # Hive is not started when there is no output file
    # or output file has no content
    while (os.path.isfile(path_to_hive_output) == False or os.stat(path_to_hive_output).st_size == 0):
        sleep(1)
    print("Hive {} started".format(host_index))

def runExperiment(num_hosts, application_path):
    topo = TestTopo(n=num_hosts)
    net = Mininet(topo=topo, host=CPULimitedHost)
    cmd_arg_string = "{}{}".format(
        " " if GO_RUN_ARGS else "",
        GO_RUN_ARGS)
    net.start()
    h0 = net.get("h0")
    host_0_address = "{}:{}".format(h0.IP(), DEFAULT_PORT)

    # Start the initial end host that all peers will connect to
    command = "go run {} -addr {} -statepath /tmp/beehive{} > beehiveOutput/0.out &".format(
        application_path,
        host_0_address,
        cmd_arg_string)
    print("Executing {} on host 0...".format(command))
    h0.cmd("export PATH=$PATH:/usr/local/go/bin")
    h0.cmd("export GOPATH=$HOME/work")
    h0.cmd(command)
    waitForHive(0)

    # Now start all the peers
    for i in range(1, num_hosts):
        host = net.get("h{}".format(i))
        host_address = "{}:{}".format(host.IP(), DEFAULT_PORT)
        command = ("go run {} -addr {} -paddrs {} -statepath /tmp/beehive{}{} > beehiveOutput/{}.out&").format(
            application_path,
            host_address,
            host_0_address,
            i,
            cmd_arg_string,
            i)
        print("Executing {} on host {}...".format(command, i))
        host.cmd("export PATH=$PATH:/usr/local/go/bin")
        host.cmd("export GOPATH=$HOME/work")
        host.cmd(command)

    # Wait for all hive to start
    for i in range(1, num_hosts):
        waitForHive(i)

    print("All hive started")
    # print("Starting CLI, press CTRL-D or type 'exit' to exit.")
    # CLI(net)
    hcon = net.get("hcon")
    host.cmd("python ./metric.py > experimentResult")
    net.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: beehive.py "
              + "[number-of-hosts]"
              + "[path/to/application.go]"
              + "[path/to/mininet/folder]\n")
        sys.exit()
    if len(sys.argv) == 4:
        os.chdir(sys.argv[3])

    try:
        num_hosts = int(sys.argv[1])
    except TypeError:
        print("{} is not a valid number of hosts\n".format(sys.argv[1]))
        sys.exit(1)

    setLogLevel("info")
    runExperiment(num_hosts, sys.argv[2])
