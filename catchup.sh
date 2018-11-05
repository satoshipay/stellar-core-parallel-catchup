#!/bin/bash

# Hacky parallel catchup
# see also https://github.com/stellar/docs/blob/3d060c0f1afb2eaff8a4076f673a8688d36e4aa5/software/known-issues.md

set -eu
set -o pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: ./catchup.sh LEDGER_MAX CHUNK_SIZE WORKERS"
  exit 1
fi

LEDGER_MAX=$1
CHUNK_SIZE=$2
WORKERS=$3

# init job queue and lock
WORKER_START=$(mktemp -t worker-start-XXXX)
WORKER_START_LOCK=$(mktemp -t worker-start-lock-XXXX)
JOB_QUEUE=$(mktemp -u -t job-queue-XXXX)
JOB_QUEUE_LOCK=$(mktemp -t job-queue-lock-XXXX)
mkfifo $JOB_QUEUE

cleanup() {
  rm $WORKER_START
  rm $WORKER_START_LOCK
  rm $JOB_QUEUE
  rm $JOB_QUEUE_LOCK
}
trap cleanup 0

log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') $1"
}

run-catchup-job() {
  JOB_ID=$1
  CATCHUP_LEDGER_MIN=$2
  CATCHUP_LEDGER_MAX=$3

  if [ "$CATCHUP_LEDGER_MIN" = "1" ]; then
    CATCHUP_AT=""
  else
    CATCHUP_AT="--catchup-at $CATCHUP_LEDGER_MIN"
  fi
  CATCHUP_TO="--catchup-to $CATCHUP_LEDGER_MAX"

  docker-compose -p catchup-job-${JOB_ID} up -d stellar-core-postgres
  sleep 30
  docker-compose -p catchup-job-${JOB_ID} run stellar-core stellar-core --conf /stellar-core.cfg --newdb
  docker-compose -p catchup-job-${JOB_ID} run stellar-core stellar-core --conf /stellar-core.cfg --newhist local
  docker-compose -p catchup-job-${JOB_ID} run -d stellar-core stellar-core --conf /stellar-core.cfg $CATCHUP_AT $CATCHUP_TO
}

worker() {
  WORKER=$1

  exec 200>$WORKER_START_LOCK
  exec 201<$JOB_QUEUE
  exec 202<$JOB_QUEUE_LOCK

  flock 200
  echo $WORKER > $WORKER_START
  flock -u 200
  exec 200<&-

  log "Worker $WORKER: started."

  while true; do
    flock 202
    read -u 201 JOB_ID JOB_LEDGER_MIN JOB_LEDGER_MAX || { log "Worker $WORKER: finished."; exit 0; }
    flock -u 202

    log "Worker $WORKER: starting job $JOB_ID ($JOB_LEDGER_MIN, $JOB_LEDGER_MAX)."
    run-catchup-job $JOB_ID $JOB_LEDGER_MIN $JOB_LEDGER_MAX
    log "Worker $WORKER: finished job $JOB_ID ($JOB_LEDGER_MIN, $JOB_LEDGER_MAX)."
  done
}

# start workers
for WORKER in $(seq 1 $WORKERS); do
  worker $WORKER &
done

MAX_JOB_ID=$(( LEDGER_MAX / CHUNK_SIZE ))
if [ "$(( MAX_JOB_ID * CHUNK_SIZE ))" -lt "$LEDGER_MAX" ]; then
  MAX_JOB_ID=$(( MAX_JOB_ID + 1 ))
fi

# job producer
{
  exec 200<$WORKER_START_LOCK
  exec 201>$JOB_QUEUE

  # wait for workers to start
  while true; do
    flock 200
    WORKERS_STARTED=$(wc -l $WORKER_START | cut -d \  -f 1)
    flock -u 200
    if [ "$WORKERS_STARTED" != "$WORKERS" ]; then
      break
    fi
    sleep 1
  done
  exec 200<&-

  # produce jobs
  for JOB_ID in $(seq 1 $MAX_JOB_ID); do
    JOB_LEDGER_MIN=$(( (JOB_ID - 1) * CHUNK_SIZE + 1))
    JOB_LEDGER_MAX=$(( JOB_ID * CHUNK_SIZE ))
    if [ "$JOB_LEDGER_MAX" -ge "$LEDGER_MAX" ]; then
      JOB_LEDGER_MAX=$LEDGER_MAX
    fi
    echo "$JOB_ID $JOB_LEDGER_MIN $JOB_LEDGER_MAX" >& 201
  done
  exec 201<&-
} &

# merge results
log "Starting result database..."
docker-compose -p catchup-result up -d stellar-core-postgres
sleep 30
docker-compose -p catchup-result run stellar-core stellar-core --conf /stellar-core.cfg --newdb

# wipe data to prevent conflicts with job 1
for TABLE in ledgerheaders txhistory txfeehistory; do
  docker-compose -p catchup-result exec -T stellar-core-postgres \
    psql stellar-core postgres -c "DELETE FROM $TABLE"
done

for JOB_ID in $(seq 1 $MAX_JOB_ID); do
  log "Waiting for job $JOB_ID..."
  while docker-compose -p catchup-job-${JOB_ID} ps stellar-core | grep stellar-core; do
    sleep 10
  done
  log "Job $JOB_ID finished."

  JOB_LEDGER_MIN=$(( (JOB_ID - 1) * CHUNK_SIZE + 1))
  JOB_LEDGER_MAX=$(( JOB_ID * CHUNK_SIZE ))
  if [ "$JOB_LEDGER_MAX" -ge "$LEDGER_MAX" ]; then
    JOB_LEDGER_MAX=$LEDGER_MAX
  fi

  if [ "$JOB_ID" != "1" ]; then
    log "Match last hash of result data with previous hash of the first ledger of job $JOB_ID"
    LAST_RESULT_LEDGER=$(( JOB_LEDGER_MIN - 1))
    LAST_RESULT_HASH=$(docker-compose -p catchup-result exec stellar-core-postgres psql -t stellar-core postgres -c "SELECT ledgerhash FROM ledgerheaders WHERE ledgerseq = $LAST_RESULT_LEDGER")
    PREVIOUS_JOB_HASH=$(docker-compose -p catchup-job-${JOB_ID} exec stellar-core-postgres psql -t stellar-core postgres -c "SELECT prevhash FROM ledgerheaders WHERE ledgerseq = $JOB_LEDGER_MIN")
    if [ "$LAST_RESULT_HASH" != "$PREVIOUS_JOB_HASH" ]; then
      log "Last result hash $LAST_RESULT_HASH (ledger $LAST_RESULT_LEDGER) does not match previous hash $PREVIOUS_JOB_HASH of first ledger of job $JOB_ID (ledger $JOB_LEDGER_MIN)"
      exit 1
    fi
  fi

  log "Merging database of job $JOB_ID in result database..."
  for TABLE in ledgerheaders txhistory txfeehistory; do
    docker-compose -p catchup-job-${JOB_ID} exec -T stellar-core-postgres \
      psql stellar-core postgres -c "COPY (SELECT * FROM $TABLE WHERE ledgerseq >= $JOB_LEDGER_MIN AND ledgerseq <= $JOB_LEDGER_MAX) TO STDOUT" |
      docker-compose -p catchup-result exec -T stellar-core-postgres \
      psql stellar-core postgres -c "COPY $TABLE FROM STDIN"
  done

  if [ "$JOB_ID" = "$MAX_JOB_ID" ]; then
    log "Copy state from job $JOB_ID to result database..."
    for TABLE in accountdata accounts ban offers peers publishqueue pubsub scphistory scpquorums signers storestate trustlines upgradehistory; do
      # wipe existing data
      docker-compose -p catchup-result exec -T stellar-core-postgres \
        psql stellar-core postgres -c "DELETE FROM $TABLE"
      # copy state
      docker-compose -p catchup-job-${JOB_ID} exec -T stellar-core-postgres \
        psql stellar-core postgres -c "COPY $TABLE TO STDOUT" |
        docker-compose -p catchup-result exec -T stellar-core-postgres \
        psql stellar-core postgres -c "COPY $TABLE FROM STDIN"
    done
  fi

  log "Merging history of job $JOB_ID..."
  docker container create --name catchup-job-${JOB_ID} -v catchup-job-${JOB_ID}_core-data:/data hello-world
  docker cp catchup-job-${JOB_ID}:/data/history ./history-${JOB_ID}
  docker rm catchup-job-${JOB_ID}
  rsync -a ./history-${JOB_ID}/ ./history-result/
  rm -rf ./history-${JOB_ID}

  # clean up job containers and volumes
  docker-compose -p catchup-job-${JOB_ID} down -v
done

wait

log "Done"
