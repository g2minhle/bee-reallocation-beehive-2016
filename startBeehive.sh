#!/bin/sh

# startBeehive.sh : Run multiple Beehive application on Mininet
# $1 : Total number of host
# $2 : Path to Beehive go application
# $3 : [Optional] Path to mininet folder

mn -c
sudo rm -rf beehiveOutput
sudo rm -rf /tmp/beehive*
mkdir beehiveOutput
sudo -E ./beehive.py $1 $2 $3