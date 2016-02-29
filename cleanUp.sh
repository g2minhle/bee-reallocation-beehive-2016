#!/bin/sh

# Clean up mininet
sudo mn -c

# Clean up temp folders 
sudo rm -rf /tmp/beehive*

# Clean up output folder
sudo rm -rf beehiveOutput

# Clean up Beehive folder
cd ../work/src/github.com/kandoo/beehive/
git fetch --all
git reset --hard origin/master