# Cache Coherence Simulator

HPC Assignment 3: a simulator for bus-based snooping cache coherence protocols, written in Python 3. It needs no extra libraries.

## How to run

```
python simulator.py inputs/benchmark1.txt
python simulator.py inputs/benchmark1.txt WRITE_ONCE
python simulator.py < inputs/spec_example.txt
```

The optional second argument overrides the `PROTOCOL` line in the file. This lets you run the same input under every protocol.

## Protocols

| Name in input file                   | States                    |
|--------------------------------------|---------------------------|
| `WRITE_INVALIDATE_WRITE_THROUGH`     | VALID, INV                |
| `WRITE_INVALIDATE_WRITE_BACK`        | RO, RW, INV               |
| `WRITE_ONCE`                         | VALID, RES, DIRTY, INV    |
| `WRITE_UPDATE_PARTIAL_WRITE_THROUGH` | VAL-X, SHARED, DIRTY (bonus)        |
| `WRITE_UPDATE_WRITE_BACK`            | VAL-X, SH-CLN, SH-DRT, DIRTY (bonus) |

## Input format

```
PROTOCOL: WRITE_INVALIDATE_WRITE_BACK
MEMORY: X=5, Y=10, Z=20      # 1 to 3 variables
PROCESSORS: P1, P2, P3       # 1 to 3 processors
P1: Read(X)
P2: Write(X, 10)
P1: Replace(X)
```

Anything after `#` is a comment.

## Output

1. A trace table. Each row has the step number and the event (Read-Hit, Read-Miss, Write-Hit, Write-Miss or Replacement), followed by the memory values and then each cache's value and state for every variable.
2. A summary. It gives the final memory and cache contents, read hits and misses, write hits and misses, bus invalidations, update broadcasts, memory write-backs and write-throughs.

## Code overview

`simulator.py` contains these classes:

- `CacheBlock`: the value and state of one variable in a cache.
- `MainMemory`: the global memory.
- `SharedBus`: sends read requests, invalidations and updates to the other caches, and counts the bus and memory traffic.
- `Cache`: a processor's cache. `read`, `write` and `replace` handle the processor's requests, and `snoop_read` and `snoop_update` handle what the cache sees on the bus.
- `CoherenceSimulator`: runs the operations and prints the trace table and summary.

## Test inputs

- `inputs/benchmark1.txt`: Test Case 1 (P, Q, X). Run it with each protocol to compare against Tables 4.4, 4.6 and 4.8.
- `inputs/benchmark2.txt`: Test Case 2 (P1, P2, P3, X, Z), which is End-of-Chapter Problem 7.
- `inputs/spec_example.txt`: the example from the assignment description.
