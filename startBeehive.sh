#!/bin/sh

# startBeehive.sh : Run multiple Beehive application on Mininet
# $1 : Total number of host
# $2 : Path to Beehive go application
# $3 : Method

# Make sure all temp folders are clean
./cleanUp.sh

# Setup output folder
mkdir beehiveOutput

# Copy all Beehive changes to beehive folder
cd ./$3
cp `ls` ../../work/src/github.com/kandoo/beehive/
cd ../

# Run the main script
sudo -E ./beehive.py $1 $2 $3

# Store experiment result
sed "s/$/,$3/" experimentResult >> experimentResult.db