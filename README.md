# NJU-DisSys-2020
## Consensus on replicated logs
Assignment of distributed system course, Fall 2020, CS@NJU.

A RAFT protocol running on single-machine implemented by Golang.

## Assignment Part1
* Task: Leader election
> go test -run Election

## Assignment Part 2
* Task: Append new log entries
> go test -run FailNoAgree\
go test -run ConcurrentStarts\
go test -run Rejoin\
go test -run Backup

## Assignment Part 3(Optional)
* Task: Handle the fault tolerant aspects of the Raft protocol
>go test -run Persist1\
go test -run Persist2\
go test -run Persist3

### Further information in assignment2020.pptx