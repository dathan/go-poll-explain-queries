## Purpose

Either in Batch or Poll, look at the process list and kill any transaction that is over X seconds, but before the query is killed, explain it if possible.
```
 ./bin/go-poll-explain-queries  --help
Usage of ./bin/go-poll-explain-queries:
  -batch
    	Only run this application once, do not act a daemon (default true)
  -kill
    	kill any slow query that bypasses the slowis threshold
  -row_threshold int
    	row_threshold is the number of rows in ready to rollback state for a long running transaction (default 1000)
  -slowis int
    	slowis the threshold in seconds that a query needs to take for it to be considered slow (default 10)
```
## Features
* Makefile to build consistently in a local environment and remote environment
* Dockerfile for a generic image to build for 
* Go Mod (which you should to your project path change)
* VS Code environment
* Generic docker push


## Installing via brew
* `brew install --verbose --build-from-source brew/Formula/go-poll-explain-queries.rb`


