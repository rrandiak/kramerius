package cz.kramerius.searchIndex.indexer.execution;

import java.util.concurrent.atomic.AtomicInteger;

public class Counters {

    private final long startTimestamp = System.currentTimeMillis();

    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicInteger indexed = new AtomicInteger(0);
    private final AtomicInteger ignored = new AtomicInteger(0);
    private final AtomicInteger removed = new AtomicInteger(0);
    private final AtomicInteger errors = new AtomicInteger(0);

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void incrementProcessed() {
        processed.incrementAndGet();
    }

    public void incrementIndexed() {
        indexed.incrementAndGet();
    }

    public void incrementIndexedBy(int count) {
        indexed.addAndGet(count);
    }

    public void incrementIgnored() {
        ignored.incrementAndGet();
    }

    public void incrementRemoved() {
        removed.incrementAndGet();
    }

    public void incrementErrors() {
        errors.incrementAndGet();
    }

    public int getProcessed() {
        return processed.get();
    }

    public int getIndexed() {
        return indexed.get();
    }

    public int getIgnored() {
        return ignored.get();
    }

    public int getRemoved() {
        return removed.get();
    }

    public int getErrors() {
        return errors.get();
    }
}