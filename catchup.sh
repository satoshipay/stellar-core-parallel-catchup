#!/bin/bash

# Hacky parallel catchup
# see also https://github.com/stellar/docs/blob/3d060c0f1afb2eaff8a4076f673a8688d36e4aa5/software/known-issues.md

set -eu
set -o pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: ./catchup.sh LEDGER_MAX WORKERS"
  exit 1
fi

LEDGER_MAX=$1
WORKERS=$2

log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') $1"
}

CHUNK_SIZE=$((LEDGER_MAX / WORKERS))

log "Starting worker databases..."
for WORKER in $(seq 1 $WORKERS); do
  docker-compose -p catchup-$WORKER up -d stellar-core-postgres
done

sleep 30

for WORKER in $(seq 1 $WORKERS); do
  log "Starting worker $WORKER..."
  WORKER_LEDGER_MIN=$(( (WORKER-1)*CHUNK_SIZE + 1 ))
  if [ "$WORKER_LEDGER_MIN" = "1" ]; then
    CATCHUP_AT=""
  else
    CATCHUP_AT="--catchup-at $WORKER_LEDGER_MIN"
  fi

  if [ "$WORKER" = "$WORKERS" ]; then
    WORKER_LEDGER_MAX="$LEDGER_MAX"
  else
    WORKER_LEDGER_MAX=$(( WORKER*CHUNK_SIZE ))
  fi
  CATCHUP_TO="--catchup-to $WORKER_LEDGER_MAX"

  docker-compose -p catchup-$WORKER run stellar-core stellar-core --conf /stellar-core.cfg --newdb
  docker-compose -p catchup-$WORKER run stellar-core stellar-core --conf /stellar-core.cfg --newhist local
  docker-compose -p catchup-$WORKER run -d stellar-core stellar-core --conf /stellar-core.cfg $CATCHUP_AT $CATCHUP_TO
done

log "Starting result database..."
docker-compose -p catchup-result up -d stellar-core-postgres
sleep 30
docker-compose -p catchup-result run stellar-core stellar-core --conf /stellar-core.cfg --newdb

for WORKER in $(seq 1 $WORKERS); do
  log "Waiting for worker $WORKER..."
  while docker-compose -p catchup-$WORKER ps stellar-core | grep stellar-core; do
    sleep 10
  done
  log "Worker $WORKER finished."

  if [ "$WORKER" = "1" ]; then
    # ledger 1 is already in the database
    WORKER_LEDGER_MIN="2"
  else
    WORKER_LEDGER_MIN=$(( (WORKER-1)*CHUNK_SIZE + 1 ))
  fi
  if [ "$WORKER" = "$WORKERS" ]; then
    WORKER_LEDGER_MAX="$LEDGER_MAX"
  else
    WORKER_LEDGER_MAX=$(( WORKER*CHUNK_SIZE ))
  fi

  if [ "$WORKER" != "1" ]; then
    log "Match last hash of result data with previous hash of the first ledger of worker $WORKER"
    LAST_RESULT_LEDGER=$(( WORKER_LEDGER_MIN - 1))
    LAST_RESULT_HASH=$(docker-compose -p catchup-result exec stellar-core-postgres psql -t stellar-core postgres -c "SELECT ledgerhash FROM ledgerheaders WHERE ledgerseq = $LAST_RESULT_LEDGER")
    PREVIOUS_WORKER_HASH=$(docker-compose -p catchup-$WORKER exec stellar-core-postgres psql -t stellar-core postgres -c "SELECT prevhash FROM ledgerheaders WHERE ledgerseq = $WORKER_LEDGER_MIN")
    if [ "$LAST_RESULT_HASH" != "$PREVIOUS_WORKER_HASH" ]; then
      log "Last result hash $LAST_RESULT_HASH (ledger $LAST_RESULT_LEDGER) does not match previous hash $PREVIOUS_WORKER_HASH of first ledger of worker $WORKER (ledger $WORKER_LEDGER_MIN)"
      exit 1
    fi
  fi

  log "Merging database of worker $WORKER in result database..."
  for TABLE in ledgerheaders txhistory txfeehistory; do
    docker-compose -p catchup-$WORKER exec -T stellar-core-postgres \
      psql stellar-core postgres -c "COPY (SELECT * FROM $TABLE WHERE ledgerseq >= $WORKER_LEDGER_MIN AND ledgerseq <= $WORKER_LEDGER_MAX) TO STDOUT" |
      docker-compose -p catchup-result exec -T stellar-core-postgres \
      psql stellar-core postgres -c "COPY $TABLE FROM STDIN"
  done

  if [ "$WORKER" = "$WORKERS" ]; then
    log "Copy state from worker $WORKER to result database..."
    for TABLE in accountdata accounts ban offers peers publishqueue pubsub scphistory scpquorums signers storestate trustlines upgradehistory; do
      # wipe existing data
      docker-compose -p catchup-result exec -T stellar-core-postgres \
        psql stellar-core postgres -c "DELETE FROM $TABLE"
      # copy state
      docker-compose -p catchup-$WORKER exec -T stellar-core-postgres \
        psql stellar-core postgres -c "COPY $TABLE TO STDOUT" |
        docker-compose -p catchup-result exec -T stellar-core-postgres \
        psql stellar-core postgres -c "COPY $TABLE FROM STDIN"
    done
  fi

  log "Merging history of worker $WORKER..."
  docker container create --name catchup-$WORKER -v catchup-${WORKER}_core-data:/data hello-world
  docker cp catchup-$WORKER:/data/history ./history-$WORKER
  docker rm catchup-$WORKER
  rsync -a ./history-$WORKER/ ./history-result/
  rm -rf ./history-$WORKER
done

log "Done"
