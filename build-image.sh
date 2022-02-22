export BEAM_VERSION=2.35.0
export BASE_IMAGE="apache/beam_python3.8_sdk:${BEAM_VERSION}"
export TAG="ver1"
export IMAGE_NAME="mybeam/beamflink"
export GOOGLE_APPLICATION_CREDENTIALS="/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Airflow/google_cloud/mlopsmac-080afb6fa874.json"


# run inside the docker
# export INPUT_AIRPORT="resources/airports.csv"
# export INPUT_FLIGHT="resources/201501_part.csv"
# export OUTPUT_BIGQUERY="mlopsmac:flights.simevents"
# export OUTPUT_FLIGHT="resources/"
# export TEMP_LOCAITON="gs://dataflow-mac/flights/temp/"



export INPUT_AIRPORT="gs://dataflow-mac/flights/airports/airports.csv.gz"
export INPUT_FLIGHT="gs://dataflow-mac/flights/raw/*.csv"
export OUTPUT_BIGQUERY="mlopsmac:flights.simevents"
export OUTPUT_FLIGHT="gs://dataflow-mac/flights/tzcorr/all_flights"
export TEMP_LOCAITON="gs://dataflow-mac/flights/temp/"
export PROJECT_ID="mlopsmac"
export REGION="us-central1"
export BUCKET="dataflow-mac"


docker pull "${BASE_IMAGE}"

#build image with buildkit
#DOCKER_BUILDKIT=1 docker build -f Dockerfile --secret id=authen_google,src=/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Airflow/google_cloud/mlopsmac-080afb6fa874.json -t "${IMAGE_NAME}:${TAG}" . 
docker build -f Dockerfile -t "${IMAGE_NAME}:${TAG}" . 


#docker run -it -v $GOOGLE_APPLICATION_CREDENTIALS:/app/mlopsmac-97d7b582eb20.json:ro -e GOOGLE_APPLICATION_CREDENTIALS=/app/mlopsmac-97d7b582eb20.json --entrypoint="/bin/bash" "${IMAGE_NAME}:${TAG}" 
#docker run -it --entrypoint="/bin/bash" "${IMAGE_NAME}:${TAG}" 


python bat_pipeline/main.py --runner=FlinkRunner \
    --job_endpoint=embed \
    --project="${PROJECT_ID}" \
    --side_input="${INPUT_AIRPORT}" \
    --input="${INPUT_FLIGHT}" \
    --output="${OUTPUT_BIGQUERY}" \
    --output_flight="${OUTPUT_FLIGHT}" \
    --temp_location="${TEMP_LOCAITON}" \
    --environment_cache_millis=10000 \
    --environment_type="DOCKER" \
    --environment_config="${IMAGE_NAME}:${TAG}" \
    --setup_file=./bat_pipeline/setup.py  # important
