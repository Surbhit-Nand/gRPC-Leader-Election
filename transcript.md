
# Part 2 — Leader Election and Duplicate Handling (Sections 3.1–3.3)

No new files needed — everything below is in `WorkerRemoteImpl.java`, already
open. About 5 minutes.

---

## Beat 11 — Starting an election

**Show:** `WorkerRemoteImpl.java:154-158`

```java
long messageId = ((long) id << 32) | (electionCounter.getAndIncrement());
seenMessageIds.add(messageId);

int currentJac = jobAllocationCounter.get();
ElectionState election = new ElectionState(this.id, this.id, currentJac);
```

**Say:**

> "When a worker starts an election, it builds a unique wave ID from its own
> worker ID shifted up 32 bits plus its personal counter — so no two workers
> can ever mint the same ID without coordinating. It votes for itself with its
> own JAC, records the wave in a map keyed by that ID, marks every neighbour
> as still outstanding, and sends the wave out."

Point at line 154, then 158. If asked why a map and not a list: "because every
lookup in the duplicate path is by message ID."

## Beat 12 — The rule: lowest JAC wins, highest ID breaks ties

**Show:** `WorkerRemoteImpl.java:191-199`

```java
if (currentJac < bestJac) {
    bestId = this.id;
    bestJac = currentJac;
} else if (currentJac == bestJac) {
    if (this.id > bestId) {
        bestId = this.id;
        bestJac = currentJac;
    }
}
```

**Say:**

> "Every worker folds itself in with the same rule: lower JAC wins, and on a
> tie the higher worker ID wins. Then it forwards the wave to every neighbour
> except the one it arrived from — that exclusion is what stops the wave
> bouncing straight back."

**Show:** `WorkerRemoteImpl.java:246` (`if (entry.getKey() != originId)`).

## Beat 13 — Echoes merge, the root announces

**Show:** `WorkerRemoteImpl.java:301-318`

**Say:**

> "As echoes come back, each neighbour's best is merged under one lock and the
> neighbour is crossed off the waiting list. When nobody is outstanding, the
> result travels back up — unless this worker started the wave, in which case
> it writes down the winner and broadcasts it to every neighbour."

**Show:** `WorkerRemoteImpl.java:332-333` (`if (election.parentId == this.id)` … `currentCoordinatorId = …`).

> "Parent equals self means I am the root. Everyone else just echoes upward.
> And each wave echoes exactly once — see the latch at lines 323 to 329."

## Beat 14 — Duplicates are answered, never dropped (memorise this)

**Show:** `WorkerRemoteImpl.java:207-221`, then `:232-239`

```java
elections.compute(messageId, (key, existing) -> {
    if (existing != null) {
        alreadySeen[0] = existing;   // duplicate: wave already tracked
        return existing;
    }
    ...
```

```java
sender.receiveEchoMessage(messageId, this.id, best[0], best[1]);
...
return;   // answered, but NOT forwarded again
```

**Say:**

> "A repeat wave is detected through this map, in one atomic step. And it is
> not thrown away — the worker reads the best candidate from the wave it is
> already tracking and echoes it straight back to the sender, without
> forwarding. Silence would stall the sender forever, because it only crosses
> a neighbour off when that neighbour's echo arrives."

Then show the codebase saying it for you:

**Show:** `WorkerElectionConvergenceTest.java:24-26`

```java
// The triangle is deliberate - two of its nodes forward the election to each other, so each
// gets a duplicate it must answer rather than drop, or those two wait on each other forever.
```

> "The test file states my thesis for me: in a triangle, two nodes send each
> other the same wave, and each must answer rather than drop, or both wait
> forever."

## Beat 15 — Announcements are the opposite: repeats are dropped

**Show:** `WorkerRemoteImpl.java:263-267`

```java
// First sighting; a broadcast that loops back to us stops here.
if (!seenCoordinatorMessageIds.add(messageId)) {
    return;
}
```

**Say:**

> "Coordinator announcements use a separate set, and here repeats genuinely
> are dropped without relay — otherwise the broadcast would circulate a cyclic
> network forever. Note this is a different set from the election one: waves
> are answered, announcements are dropped. Mixing those two up is the classic
> wrong answer."

## Beat 16 — Why cycles make all of this necessary (closing line for Part 2)

**Show:** nothing — then `WorkerElectionConvergenceTest.java:111` (`wireTriangle`) if pressed.

**Say:**

> "The bootstrap hands each newcomer one random peer, so the network grows
> into cycles — triangles, not rings. In a triangle the wave comes back
> through the other path looking brand new, which is why detection has to be
> exact and answers mandatory. Answer once, echo once, relay announcements
> once — that is the whole termination argument, and the convergence tests
> prove it across triangle, chain and five-worker topologies."

---

## Rehearsal notes

- Memorise three sentences word-for-word: the `putIfAbsent` one (Beat 5), the
  "no send-me-your-members call anywhere" one (Beat 9), and "each must answer
  rather than drop, or those two wait on each other forever" (Beat 14). Those
  are the distinction-grade moments; everything else can be paraphrased.
- If the lecturer interrupts with "show me," your finger is already on the
  line — every claim lists its lines.
- Caveats to keep in mind if questioned:
  - the bootstrap prunes dead entries lazily on the next join
    (`BootstrapRegistryImpl.java:61-63`), it is not a liveness monitor;
  - the duplicate-ID rejection path has code but no dedicated test;
  - `seenMessageIds` (`WorkerRemoteImpl.java:60`) is an audit record and
    explicitly *not* the dedup guard — the guard is the `elections` map
    (`:208-221`). If asked "where do you check for duplicates," point at the
    map, never the set.
