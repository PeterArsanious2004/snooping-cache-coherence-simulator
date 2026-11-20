"""
Bus-based snooping cache coherence simulator
HPC - Assignment 3

How to run:
    python simulator.py input.txt
    python simulator.py input.txt WRITE_ONCE     (use a different protocol than the file)
    python simulator.py < input.txt              (read the input from stdin)

Supported protocols:
    WRITE_INVALIDATE_WRITE_THROUGH       states: VALID, INV
    WRITE_INVALIDATE_WRITE_BACK          states: RO, RW, INV
    WRITE_ONCE                           states: VALID, RES, DIRTY, INV
    WRITE_UPDATE_PARTIAL_WRITE_THROUGH   states: VAL-X, SHARED, DIRTY          (bonus)
    WRITE_UPDATE_WRITE_BACK              states: VAL-X, SH-CLN, SH-DRT, DIRTY  (bonus)
"""

import re
import sys

WT = "WRITE_INVALIDATE_WRITE_THROUGH"
WB = "WRITE_INVALIDATE_WRITE_BACK"
WO = "WRITE_ONCE"
WU_PWT = "WRITE_UPDATE_PARTIAL_WRITE_THROUGH"
WU_WB = "WRITE_UPDATE_WRITE_BACK"
PROTOCOLS = [WT, WB, WO, WU_PWT, WU_WB]

# States where memory is out of date, so the block has to be
# written back to memory when it gets replaced.
DIRTY_STATES = {
    WT: [],
    WB: ["RW"],
    WO: ["DIRTY"],
    WU_PWT: ["DIRTY"],
    WU_WB: ["DIRTY", "SH-DRT"],
}


class CacheBlock:
    # one variable = one block, the variable name is used as the tag
    def __init__(self, value, state):
        self.value = value
        self.state = state


class MainMemory:
    def __init__(self, values):
        self.values = dict(values)

    def read(self, var):
        return self.values[var]

    def write(self, var, value):
        self.values[var] = value


class SharedBus:
    """Connects all caches with the main memory.
    Every message sent on the bus is seen (snooped) by all the other caches."""

    def __init__(self, memory):
        self.memory = memory
        self.caches = []
        # statistics
        self.invalidations = 0
        self.updates = 0
        self.write_backs = 0
        self.write_throughs = 0

    def other_caches(self, sender):
        return [c for c in self.caches if c is not sender]

    def broadcast_read(self, var, requester, for_write=False):
        # Read request for a miss. Memory answers unless a cache supplies the data
        # (the owner in the invalidate protocols, any copy in the update protocols).
        # Also returns whether another cache has a copy of the block.
        value = self.memory.read(var)
        shared = False
        for cache in self.other_caches(requester):
            if cache.has_valid(var):
                shared = True
                data = cache.snoop_read(var, for_write)
                if data is not None:
                    value = data
        return value, shared

    def broadcast_invalidate(self, var, sender):
        self.invalidations += 1
        for cache in self.other_caches(sender):
            if cache.has_valid(var):
                cache.blocks[var].state = "INV"

    def broadcast_update(self, var, value, sender):
        # write-update: send the new value to every cache that has a copy
        self.updates += 1
        shared = False
        for cache in self.other_caches(sender):
            if cache.has_valid(var):
                cache.snoop_update(var, value)
                shared = True
        return shared

    def write_through(self, var, value):
        self.memory.write(var, value)
        self.write_throughs += 1

    def write_back_to_memory(self, var, value):
        self.memory.write(var, value)
        self.write_backs += 1


class Cache:
    """Private cache of one processor, together with its cache controller."""

    def __init__(self, name, protocol, bus):
        self.name = name
        self.protocol = protocol
        self.bus = bus
        self.blocks = {}
        bus.caches.append(self)

    def has_valid(self, var):
        block = self.blocks.get(var)
        return block is not None and block.state != "INV"

    # ----- requests coming from the processor -----

    def read(self, var):
        if self.has_valid(var):
            return "Read-Hit"

        value, shared = self.bus.broadcast_read(var, self)
        if self.protocol == WB:
            state = "RO"
        elif self.protocol == WU_PWT:
            state = "SHARED" if shared else "VAL-X"
        elif self.protocol == WU_WB:
            state = "SH-CLN" if shared else "VAL-X"
        else:
            state = "VALID"
        self.blocks[var] = CacheBlock(value, state)
        return "Read-Miss"

    def write(self, var, value):
        if self.has_valid(var):
            self.write_hit(var, value)
            return "Write-Hit"
        self.write_miss(var, value)
        return "Write-Miss"

    def write_hit(self, var, value):
        bus = self.bus
        block = self.blocks[var]
        block.value = value

        if self.protocol == WT:
            bus.broadcast_invalidate(var, self)
            bus.write_through(var, value)

        elif self.protocol == WB:
            # RO: take ownership first.  RW: we already own it, no bus traffic.
            if block.state == "RO":
                bus.broadcast_invalidate(var, self)
                block.state = "RW"

        elif self.protocol == WO:
            if block.state == "VALID":
                # first write goes through to memory
                bus.broadcast_invalidate(var, self)
                bus.write_through(var, value)
                block.state = "RES"
            else:
                # second write and after: write back, stays local
                block.state = "DIRTY"

        elif self.protocol == WU_PWT:
            if block.state == "SHARED":
                still_shared = bus.broadcast_update(var, value, self)
                bus.write_through(var, value)
                block.state = "SHARED" if still_shared else "VAL-X"
            else:
                block.state = "DIRTY"

        elif self.protocol == WU_WB:
            if block.state in ("SH-CLN", "SH-DRT"):
                still_shared = bus.broadcast_update(var, value, self)
                block.state = "SH-DRT" if still_shared else "DIRTY"
            else:
                block.state = "DIRTY"

    def write_miss(self, var, value):
        bus = self.bus
        # bring the block in first (from memory or from another cache)
        _, shared = bus.broadcast_read(var, self, for_write=True)

        if self.protocol == WT:
            bus.broadcast_invalidate(var, self)
            bus.write_through(var, value)
            state = "VALID"
        elif self.protocol == WB:
            bus.broadcast_invalidate(var, self)
            state = "RW"
        elif self.protocol == WO:
            bus.broadcast_invalidate(var, self)
            state = "DIRTY"
        elif self.protocol == WU_PWT:
            if shared:
                bus.broadcast_update(var, value, self)
                bus.write_through(var, value)
                state = "SHARED"
            else:
                state = "DIRTY"
        else:  # WU_WB
            if shared:
                bus.broadcast_update(var, value, self)
                state = "SH-DRT"
            else:
                state = "DIRTY"

        self.blocks[var] = CacheBlock(value, state)

    def replace(self, var):
        block = self.blocks.pop(var, None)
        if block is not None and block.state in DIRTY_STATES[self.protocol]:
            self.bus.write_back_to_memory(var, block.value)
            return "Replacement, Write-Back"
        return "Replacement"

    # ----- requests snooped from the bus -----

    def snoop_read(self, var, for_write):
        # Another cache missed on var. Return the value if this cache
        # supplies it, otherwise None (memory will supply it).
        block = self.blocks[var]

        if self.protocol == WT:
            return None

        if self.protocol in (WB, WO):
            owner_state = "RW" if self.protocol == WB else "DIRTY"
            if block.state == owner_state:
                if not for_write:
                    # read miss: owner updates memory and both caches share the block
                    self.bus.write_back_to_memory(var, block.value)
                    block.state = "RO" if self.protocol == WB else "VALID"
                # on a write miss the INV that follows will invalidate us
                return block.value
            if block.state == "RES" and not for_write:
                block.state = "VALID"  # we are not the only copy anymore
            return None

        # update protocols: any cache that has the block can supply it
        if not for_write:
            if self.protocol == WU_PWT:
                if block.state == "DIRTY":
                    self.bus.write_back_to_memory(var, block.value)
                block.state = "SHARED"
            else:
                if block.state in ("DIRTY", "SH-DRT"):
                    block.state = "SH-DRT"
                else:
                    block.state = "SH-CLN"
        return block.value

    def snoop_update(self, var, value):
        block = self.blocks[var]
        block.value = value
        block.state = "SHARED" if self.protocol == WU_PWT else "SH-CLN"


class CoherenceSimulator:
    def __init__(self, protocol, memory, processors, operations):
        self.protocol = protocol
        self.variables = list(memory)
        self.processors = processors
        self.operations = operations

        self.memory = MainMemory(memory)
        self.bus = SharedBus(self.memory)
        self.caches = {}
        for p in processors:
            self.caches[p] = Cache(p, protocol, self.bus)

        self.counts = {"Read-Hit": 0, "Read-Miss": 0, "Write-Hit": 0, "Write-Miss": 0}
        self.trace = []
        self.save_row("Initial State")

    def save_row(self, event):
        # one row of the trace table: serial, event, memory, then (value, state) per cache
        row = [str(len(self.trace)), event]
        for v in self.variables:
            row.append(str(self.memory.read(v)))
        for p in self.processors:
            for v in self.variables:
                block = self.caches[p].blocks.get(v)
                if block is None:
                    row += ["-", "-"]
                else:
                    row += [str(block.value), block.state]
        self.trace.append(row)

    def run_all(self):
        for proc, op, var, value in self.operations:
            cache = self.caches[proc]
            if op == "READ":
                result = cache.read(var)
                event = f"{proc} reads {var} ({result})"
            elif op == "WRITE":
                result = cache.write(var, value)
                event = f"{proc} updates {var}={value} ({result})"
            else:
                result = cache.replace(var)
                event = f"{proc} replaces {var} ({result})"

            if result in self.counts:
                self.counts[result] += 1
            self.save_row(event)

    def print_trace_table(self):
        n = len(self.variables)
        groups = [("Serial", 1), ("Event", 1), ("Memory", n)]
        names = ["", ""] + self.variables
        for p in self.processors:
            groups.append((p + " Cache", 2 * n))
            for v in self.variables:
                names += [f"Val({v})", f"State({v})"]

        # width of every column
        widths = []
        for i in range(len(names)):
            widths.append(max(len(row[i]) for row in [names] + self.trace))

        # make the last column of a group wider if the group title doesn't fit
        start = 0
        for title, size in groups:
            group_width = sum(widths[start:start + size]) + 2 * (size - 1)
            if len(title) > group_width:
                widths[start + size - 1] += len(title) - group_width
            start += size

        def format_row(cells):
            parts = []
            start = 0
            for _, size in groups:
                cols = range(start, start + size)
                parts.append("  ".join(cells[i].ljust(widths[i]) for i in cols))
                start += size
            return " | ".join(parts).rstrip()

        names_line = format_row(names)
        titles_line = " | ".join(title.ljust(len(part)) for (title, _), part
                                 in zip(groups, names_line.split(" | "))).rstrip()
        rows = [format_row(row) for row in self.trace]
        line = "=" * len(max(rows + [names_line], key=len))

        print("Protocol:", self.protocol)
        print(line)
        print(titles_line)
        print(names_line)
        print(line)
        for r in rows:
            print(r)
        print(line)

    def print_summary(self):
        print("\nFinal state:")
        for v in self.variables:
            text = f"  {v}:  Memory = {self.memory.read(v):<5}"
            for p in self.processors:
                block = self.caches[p].blocks.get(v)
                if block is None:
                    text += f"  {p} = -"
                else:
                    text += f"  {p} = {block.value} {block.state}"
            print(text)

        print("\nStatistics:")
        print(f"  Read hits / read misses       : {self.counts['Read-Hit']} / {self.counts['Read-Miss']}")
        print(f"  Write hits / write misses     : {self.counts['Write-Hit']} / {self.counts['Write-Miss']}")
        print(f"  Bus invalidation commands     : {self.bus.invalidations}")
        print(f"  Bus update broadcasts         : {self.bus.updates}")
        print(f"  Memory write-backs            : {self.bus.write_backs}")
        print(f"  Write-through memory updates  : {self.bus.write_throughs}")


def error(msg):
    print("Error:", msg)
    sys.exit(1)


def load_input(text, protocol=None):
    memory = None
    processors = None
    operations = []

    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.split("#")[0].strip()   # remove comments
        if line == "":
            continue

        key, _, rest = line.partition(":")
        key = key.strip()
        rest = rest.strip()

        if key.upper() == "PROTOCOL":
            if protocol is None:
                protocol = rest.upper()

        elif key.upper() == "MEMORY":
            memory = {}
            for item in rest.split(","):
                name, value = item.split("=")
                memory[name.strip()] = int(value)

        elif key.upper() == "PROCESSORS":
            processors = [p.strip() for p in rest.split(",")]

        else:
            # an operation like  P1: Write(X, 10)
            match = re.fullmatch(r"(\w+)\s*\(\s*(\w+)\s*(,\s*(-?\d+)\s*)?\)", rest)
            if not match:
                error(f"line {line_no}: can't read '{line}'")
            op = match.group(1).upper()
            var = match.group(2)
            value = match.group(4)

            if processors is None or key not in processors:
                error(f"line {line_no}: unknown processor '{key}'")
            if memory is None or var not in memory:
                error(f"line {line_no}: unknown variable '{var}'")
            if op not in ("READ", "WRITE", "REPLACE"):
                error(f"line {line_no}: unknown operation '{match.group(1)}'")
            if op == "WRITE" and value is None:
                error(f"line {line_no}: Write needs a value")
            if op != "WRITE" and value is not None:
                error(f"line {line_no}: only Write takes a value")

            if value is not None:
                value = int(value)
            operations.append((key, op, var, value))

    if protocol not in PROTOCOLS:
        error(f"unknown protocol '{protocol}', choose one of: " + ", ".join(PROTOCOLS))
    if not memory or len(memory) > 3:
        error("MEMORY must have 1 to 3 variables")
    if not processors or len(processors) > 3:
        error("PROCESSORS must have 1 to 3 processors")

    return CoherenceSimulator(protocol, memory, processors, operations)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            text = f.read()
    else:
        text = sys.stdin.read()

    chosen_protocol = None
    if len(sys.argv) > 2:
        chosen_protocol = sys.argv[2].upper()

    sim = load_input(text, chosen_protocol)
    sim.run_all()
    sim.print_trace_table()
    sim.print_summary()
