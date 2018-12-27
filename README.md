# Parallel Stellar Core Catchup ⚡

## Background

### Goal

Sync a full Stellar validator node (including full history) as fast as possible.

### Problem

A full catchup takes weeks/months – even without publishing to an archive.

### Idea

 * Split the big ledger into small chunks of size `CHUNK_SIZE`.
 * Run a catchup for the chunks in parallel with `WORKERS` worker processes.
 * Stitch together the resulting database and history archive.

## Usage

```
./catchup.sh DOCKER_COMPOSE_FILE LEDGER_MIN LEDGER_MAX CHUNK_SIZE WORKERS
```

Arguments:

* `DOCKER_COMPOSE_FILE`: use `docker-compose.pubnet.yaml` for the public network (`docker-compose.testnet.yaml` for testnet).
* `LEDGER_MIN`: smallest ledger number you want. Use `1` for doing a full sync.
* `LEDGER_MAX`: largest ledger number you want, usually you'll want the latest one which is exposed as `core_latest_ledger` in any synced Horizon server, e.g. https://stellar-horizon.satoshipay.io/.
* `CHUNK_SIZE`: number of ledgers to work on in one worker.
* `WORKERS`: number of workers that should be spawned. For best performance this should not exceed the number of CPUs.

## Hardware sizing and timing examples

* On Google Cloud a full sync took less than 24h on a `n1-standard-32` machine (32 CPUs, 120GB RAM, 1TB SSD) with a `CHUNK_SIZE` of `32768` and 32 workers (see below).
* ... add your achieved result here by submitting a PR.

## Example run on dedicated Google Cloud machine

```
sudo apt-get update
sudo apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg2 \
  software-properties-common \
  python-pip
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository \
  "deb [arch=amd64] https://download.docker.com/linux/debian \
  $(lsb_release -cs) \
  stable"
sudo apt-get update
sudo apt-get install -y docker-ce
sudo pip install docker-compose
echo '{"default-address-pools":[{"base":"172.80.0.0/16","size":29}]}' | sudo tee /etc/docker/daemon.json
sudo usermod -G docker andre
sudo reboot
# log in again and check whether docker works
docker ps
```

```
git clone git@github.com:satoshipay/stellar-core-parallel-catchup.git
cd stellar-core-parallel-catchup
./catchup.sh docker-compose.pubnet.yaml 1 20971520 32768 32 2>&1 | tee catchup.log
```

You will get 3 important pieces of data for Stellar Core:

* SQL database: if you need to move the data to another container/machine you can dump the database by running the following:

    ```
    docker exec catchup-result_stellar-core-postgres_1 pg_dump -F d -f catchup-sqldump -j 10 -U postgres -d stellar-core
    ```

    Then copy the `catchup-sqldump` directory to the target container/machine and restore with `pg_restore`.

* `data-result` directory: contains the `buckets` directory that Stellar Core needs for continuing with the current state in the SQL database.
* `history-result` directory: contains the full history that can be published to help other validator nodes to catch up (e.g., S3, GCS, IPFS, or any other file storage).

Note: make sure you have a consistent state of the three pieces of data before starting Stellar Core in SCP mode (e.g., when moving data to another machine).

## Reset

If you need to start from scratch again you can delete all docker-compose projects:

```
for PROJECT in $(docker ps --filter "label=com.docker.compose.project" -q | xargs docker inspect --format='{{index .Config.Labels "com.docker.compose.project"}}'| uniq | grep catchup-); do docker-compose -f docker-compose.pubnet.yaml -p $PROJECT down -v; done
docker volume prune
```
