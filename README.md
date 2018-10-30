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
