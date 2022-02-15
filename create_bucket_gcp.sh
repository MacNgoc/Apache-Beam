#!/bin/bash
echo "Creating bucket for flight project"

export BUCKET=${BUCKET:=dataflow-mac}

#gsutil mb -p mlopsmac -l us-central1 gs://$BUCKET
gsutil cp /Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Apache_Beam/Airline_project/data/201501_part.csv gs://$BUCKET/flights/raw/

gsutil cp /Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Apache_Beam/Airline_project/data/airports.csv.gz gs://$BUCKET/flights/airports/

bq --location=us-central1 mk --description "creating dataset to store flight data" flights
#gsutil -m cp *.csv gs://$BUCKET/flights/raw
#how to run a command after the previous finished