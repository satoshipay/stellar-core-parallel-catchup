# (Hacky!) Parallel Stellar Core Catchup

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

If you need to start from scratch again you can delete all docker-compose projects:

```
for PROJECT in $(docker ps --filter "label=com.docker.compose.project" -q | xargs docker inspect --format='{{index .Config.Labels "com.docker.compose.project"}}'| uniq | grep catchup-); do docker-compose -f docker-compose.pubnet.yaml -p $PROJECT down -v; done
docker volume prune
```

## Fast sync on dedicated gcloud machine

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
git clone git@gitlab.satoshipay.tech:stellar/parallel-catchup.git
cd parallel-catchup
./catchup.sh docker-compose.pubnet.yaml 1 20971520 32768 32 2>&1 | tee catchup.log
docker exec catchup-result_stellar-core-postgres_1 pg_dump -F d -f catchup-sqldump -U postgres -d stellar-core
```
