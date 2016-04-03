#!/bin/sh

# startBeehive.sh : Run multiple Beehive application on Mininet
# $1 : Total number of host
# $2 : Path to Beehive go application
# $3 : Method

if [ $# -ne 3 ]
    then
        echo "Not enough arguments given"
        exit 1
fi

# Make sure all temp folders are clean
./cleanUp.sh

# Setup output folder
mkdir beehiveOutput

# Copy all Beehive changes to beehive folder
cd ./$3
cp `ls` ${GOPATH}/src/github.com/kandoo/beehive/
cd ../

# Run the main script
sudo python -E ./beehive.py $1 $2 $3

# Store experiment result
sed "s/$/,$3/" experimentResult.csv >> experimentResultDB.csv
