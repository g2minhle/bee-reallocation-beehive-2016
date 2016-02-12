"""
A script to create a mininet topology with a given number of hosts
connected by one switch, and run a beehive cluster on them.

Put this script in your ~/mininet folder (otherwise it won't be able
to import from the mininet Python API). Run it as follows:

sudo -E python beehive.py [number-of-hosts] [path/to/application.go]

THE -E FLAG IS IMPORTANT. If it is not set, your environment variables
will not be preserved while running mininet, so the hosts won't be able
to find your GOPATH.

So far the only way to actually see output from a hive is to run something
like "h1 echo $GOPATH" in the mininet CLI which will echo the GOPATH as well
as whatever else has been printed in h1's console.

You can edit GO_RUN_ARGS to include any arguments you want to pass to
your application EXCEPT -addr, -paddrs, and -statepath.
"""

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.log import setLogLevel
from mininet.cli import CLI

import sys
from time import sleep

DEFAULT_PORT = 7677
GO_RUN_ARGS = ""


class TestTopo(Topo):
    """n hosts connected by a single switch."""

    def build(self, n=2):
        switch = self.addSwitch("s1")

        for h in range(1, n + 1):
            host = self.addHost("h{}".format(h))
            self.addLink(host, switch)


def test(num_hosts, application_path):
    topo = TestTopo(n=num_hosts)
    net = Mininet(topo)
    net.start()

    h1 = net.get("h1")
    host_1_address = "{}:{}".format(h1.IP(), DEFAULT_PORT)

    cmd_arg_string = "{}{}".format(
        " " if GO_RUN_ARGS else "",
        GO_RUN_ARGS)

    # Start the initial end host that all peers will connect to
    command = "go run {} -addr {} -statepath /tmp/beehive{} &".format(
        application_path,
        host_1_address,
        cmd_arg_string)
    print("Executing {} on host 1...".format(command))
    h1.cmd(command)

    # Wait a bit for the application to start up
    print("Waiting for the application to start up...")
    sleep(5)

    # Now start all the peers
    for i in range(2, num_hosts + 1):
        host = net.get("h{}".format(i))
        host_address = "{}:{}".format(host.IP(), DEFAULT_PORT + i)
        command = ("go run {} -addr {} -paddrs {}"
                   " -statepath /tmp/beehive{} {} &").format(
            application_path,
            host_address,
            host_1_address,
            i,
            cmd_arg_string)
        print("Executing {} on host {}...".format(command, i))
        host.cmd(command)

    print("Starting CLI, press CTRL-D or type 'exit' to exit.")
    CLI(net)

    net.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {} [number-of-hosts] [path/to/application.go]\n".format(
            sys.argv[0]))
        sys.exit()

    try:
        num_hosts = int(sys.argv[1])
    except TypeError:
        num_hosts = -1

    if num_hosts < 1:
        print("{} is not a valid number of hosts\n".format(sys.argv[1]))
        sys.exit()

    setLogLevel("info")
    test(num_hosts, sys.argv[2])