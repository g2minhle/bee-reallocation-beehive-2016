#!/usr/bin/python
"""
A script to create a mininet topology with a given number of hosts
connected by one switch, and run a beehive cluster on them.

Put this script in your ~/mininet folder or give this script the path
to mininet folder (otherwise it won't be able to import from the
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
        """Set up n hives with one controller host."""

        # CPU limit of a non-controller host.
        # The controller will get 50% CPU, and the remaining
        # 50% will be split among the other hosts.
        cpu_limit = .5 / (n + 1)

        switch = self.addSwitch("s1")

        # The main controller host
        host = self.addHost(CONTROLLER_HOST_NAME)
        self.addLink(host, switch)

        # n hives connected to the controller
        for h in range(0, n):
            host = self.addHost("h{}".format(h), cpu=cpu_limit)
            self.addLink(host, switch)


def wait_for_hive(hive_index):
    """Busy-wait for the application on hive number hive_index to start up."""

    print("Waiting for {} hive to start ...".format(hive_index))

    path_to_hive_output = "beehiveOutput/{}.out".format(hive_index)

    # Hive is not started when there is no output file
    # or output file has no content
    while (os.path.isfile(path_to_hive_output) == False or
           os.stat(path_to_hive_output).st_size == 0):
        sleep(1)

    print("Hive {} started".format(hive_index))


def get_full_host_address(host, port=None):
    """
    Return the formatted IP address and port number of host.
    host should be an instance of class Host.
    If a port number is supplied, use that; otherwise, use the default
    port number.
    """

    if port is None:
        port = DEFAULT_PORT

    return "{}:{}".format(host.IP(), port)


def getRunCommand(host, id, application_path, peer_list=None):
    """
    Return a command string that, when executed by host, starts up the
    Beehive application located at application_path.
    id is a unique identifier forhost.
    """

    go_run_app = "go run {}".format(application_path)
    host_address = "-addr {}".format(get_full_host_address(host))

    # Construct a list of peer addresses for the application
    peer_addresses = ""
    if peer_list is not None:
        peer_addresses = "-paddrs {}".format(
            " ".join(get_full_host_address(peer) for peer in peer_list))

    state_path = "-statepath /tmp/beehive{}".format(id)
    stdout_output = "> beehiveOutput/{}.out".format(id)
    stderror_output = "2> beehiveOutput/{}.error.out".format(id)

    # Put all the command-line arguments together
    return " ".join([
        go_run_app,
        host_address,
        peer_addresses,
        state_path,
        GO_RUN_ARGS,
        stdout_output,
        stderror_output,
        '&'])


def run_experiment(num_hosts, application_path):
    topo = TestTopo(n=num_hosts)
    net = Mininet(topo=topo, host=CPULimitedHost)
    net.start()
    h0 = net.get("h0")

    # Start the initial end host that all peers will connect to
    command = getRunCommand(h0, 0, application_path)
    print("Executing {} on host {}...".format(command, 0))
    h0.cmd(command)
    wait_for_hive(0)

    # Now start all the peers
    for i in range(1, num_hosts):
        host = net.get("h{}".format(i))
        command = getRunCommand(host, i, application_path, [h0])
        print("Executing {} on host {}...".format(command, i))
        host.cmd(command)

    # Wait for all peers to start
    for i in range(1, num_hosts):
        wait_for_hive(i)

    print("All hives started")

    # Run the experiment and save the results to a file
    hcon = net.get("hcon")
    hcon.cmd("python ./metric.py >> experimentStdout 2>> experimentStderr")
    net.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: beehive.py " +
              "[number-of-hosts]" +
              "[path/to/application.go]")
        sys.exit()

    try:
        num_hosts = int(sys.argv[1])
    except TypeError:
        print("{} is not a valid number of hosts\n".format(sys.argv[1]))
        sys.exit(1)

    setLogLevel("info")
    run_experiment(num_hosts, sys.argv[2])
