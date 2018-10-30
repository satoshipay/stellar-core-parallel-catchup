# (Hacky!) Parallel Stellar Core Catchup

## Background

### Goal

Sync a full Stellar validator node (including full history) as fast as possible.

### Problem

A full catchup takes weeks/months – even without publishing to an archive.

### Idea

 * Split the big ledger into small chunks.
 * Run a catchup for the chunks in parallel.
 * Stitch together the resulting database and history archive.

## Usage

```
./catchup.sh LEDGER_MAX WORKERS
```

If you need to start from scratch again you can delete all docker-compose projects:

```
docker-compose -p catchup-result down -v
docker-compose -p catchup-1 down -v
docker-compose -p catchup-2 down -v
...
```
