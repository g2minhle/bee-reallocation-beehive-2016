#!/bin/sh

# Clean up mininet
sudo mn -c

# Clean up temp folders
sudo rm -rf /tmp/*

# Clean up output folder
sudo rm -rf ./out

# Clean up Beehive folder
cd ${GOPATH}/src/github.com/kandoo/beehive/
git fetch --all
git reset --hard origin/master
