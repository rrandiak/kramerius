package cz.incad.kramerius.utils.solr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import java.util.logging.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Lock-free, high-throughput batch updater for Solr using a ring buffer.
 * 
 * <p>This class provides a non-blocking mechanism for updating Solr documents efficiently.
 * Producer threads (calling {@link #add(SolrInputDocument)}) never block and push documents
 * into an internal lock-free ring buffer. Consumer tasks process batches of documents
 * asynchronously using a fixed-size thread pool, up to a configurable parallelism limit.</p>
 * 
 * <p>Behavior highlights:</p>
 * <ul>
 *     <li><b>Lock-free producers:</b> Documents are added using atomic CAS operations,
 *         ensuring no thread ever blocks.</li>
 *     <li><b>Batch processing:</b> Documents are sent to Solr in batches for higher throughput,
 *         reducing the overhead of individual updates.</li>
 *     <li><b>Fallback updates:</b> If the buffer is full or a batch fails, documents are
 *         immediately updated individually in Solr. This ensures no documents are lost
 *         and naturally throttles producers under high load.</li>
 *     <li><b>Bounded parallelism:</b> The {@code inFlight} counter enforces the maximum
 *         number of concurrent batch tasks, avoiding overloading Solr or the thread pool.</li>
 *     <li><b>Safe shutdown:</b> The updater flushes all remaining documents and waits
 *         for in-flight tasks to complete before terminating the executor service.</li>
 * </ul>
 * 
 * <p><b>Custom error handling:</b> To implement custom behavior for failed updates,
 * this class can be extended and the private methods {@link #updateSingle(SolrInputDocument)}
 * and {@link #updateBatch(List)} can be overridden in the subclass.</p>
 * 
 * <p>This design ensures maximum throughput, stability under heavy load, and efficient
 * use of system resources, while maintaining full non-blocking semantics for producers.</p>
 */
public abstract class AbstractSolrBatchUpdater {
    public static final Logger LOGGER = Logger.getLogger(AbstractSolrBatchUpdater.class.getName());

    private static final int MAX_CAS_ATTEMPTS_OFFER = 3;
    private static final int MAX_CAS_ATTEMPTS_POLL = 15;
    private static final int MAX_CAS_ATTEMPTS_INFLIGHT = 3;

    private final int capacity;
    private final int batchSize;
    private final int maxParallel;

    protected final SolrClient solrClient;

    private final SolrInputDocument[] buffer;
    private final ExecutorService executor;

    private final AtomicInteger head = new AtomicInteger(0);
    private final AtomicInteger tail = new AtomicInteger(0);
    private final AtomicInteger inFlight = new AtomicInteger(0);

    /**
     * Creates a new SolrBatchUpdater.
     *
     * @param batchSize the maximum number of documents per batch sent to Solr
     * @param numParallel the maximum number of concurrent batch update threads
     * @param solrClient the SolrClient instance used for updates
     */
    public AbstractSolrBatchUpdater(int batchSize, int numParallel, SolrClient solrClient) {
        this.capacity = 2 * batchSize * numParallel;
        this.batchSize = batchSize;
        this.maxParallel = numParallel;

        this.solrClient = solrClient;

        this.buffer = new SolrInputDocument[capacity];

        this.executor = Executors.newFixedThreadPool(numParallel);
    }

    /**
     * Attempts to insert a document into the ring buffer in a non-blocking manner.
     * Uses {@link #MAX_CAS_ATTEMPTS_OFFER} to limit CAS retries.
     *
     * @param doc the Solr document to insert
     * @return true if the document was added to the buffer, false if the buffer was full
     */
    private boolean offer(SolrInputDocument doc) {
        int currentTail;
        int nextTail;
        int casAttempts = 0;

        do {
            currentTail = tail.get();
            nextTail = (currentTail + 1) % capacity;

            if (nextTail == head.get()) {
                // buffer full
                return false;
            }

            if (tail.compareAndSet(currentTail, nextTail)) {
                // successfully reserved a slot
                buffer[currentTail] = doc;
                return true;
            }

            casAttempts++;
        } while (casAttempts < MAX_CAS_ATTEMPTS_OFFER);

        // failed after max attempts
        return false;
    }

    /**
     * Polls a single document from the buffer in a non-blocking manner.
     * Uses {@link #MAX_CAS_ATTEMPTS_POLL} to limit CAS retries.
     *
     * @return a document if available, or {@code null} if the buffer is empty
     */
    private SolrInputDocument poll() {
        int currentHead;
        int nextHead;
        int casAttempts = 0;

        do {
            currentHead = head.get();
            if (currentHead == tail.get()) {
                // buffer empty
                return null;
            }

            nextHead = (currentHead + 1) % capacity;

            if (head.compareAndSet(currentHead, nextHead)) {
                SolrInputDocument doc = buffer[currentHead];
                buffer[currentHead] = null;
                return doc;
            }

            casAttempts++;
        } while (casAttempts < MAX_CAS_ATTEMPTS_POLL);

        // Failed to update head after max attempts
        return null;
    }

    /**
     * Polls up to {@link #batchSize} documents from the buffer for batch updates.
     *
     * @return a list of documents (may be empty if buffer is empty)
     */
    private List<SolrInputDocument> pollBatch() {
        List<SolrInputDocument> batch = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            SolrInputDocument doc = poll();

            if (doc == null) {
                break;
            }

            batch.add(doc);
        }

        return batch;
    }

    /**
     * Returns the number of documents currently available in the buffer.
     *
     * @return number of documents in buffer
     */
    private int size() {
        int diff = tail.get() - head.get();
        return diff >= 0 ? diff : diff + capacity;
    }

    /**
     * Updates a single document in Solr. Used as a fallback if the buffer is full
     * or a batch update fails.
     *
     * @param doc the Solr document to update
     */
    protected abstract void updateSingle(SolrInputDocument doc);

    /**
     * Updates a batch of documents in Solr. If the batch fails, falls back
     * to updating each document individually.
     *
     * @param docs list of documents to update
    */
    protected abstract void updateBatch(List<SolrInputDocument> docs);

    /**
     * Attempts to dispatch a batch update task to the executor.
     * 
     * <p>This method enforces the {@link #maxParallel} limit on concurrent tasks. Each
     * dispatched task processes one or more batches of documents from the buffer until
     * fewer than {@link #batchSize} documents remain, ensuring efficient batch updates
     * without overwhelming the system.</p>
     * 
     * <p>Lock-free behavior:</p>
     * <ul>
     *     <li>Producers never block when adding documents to the buffer.</li>
     *     <li>Consumers check the {@code inFlight} counter in a lock-free manner before
     *         submitting tasks to the {@link ExecutorService}. To prevent task flooding, the
     *        increment of {@code inFlight} uses a limited number of CAS attempts.</li>
     *     <li>The internal locks of {@link ExecutorService} are never contended in this
     *         design, because tasks are only submitted when allowed by {@code inFlight},
     *         so no thread ever waits for a lock.</li>
     * </ul>
     * 
     * <p>This combination ensures that both document producers and batch consumers
     * operate efficiently under high throughput, while automatically throttling
     * the submission of tasks to avoid exceeding the configured parallelism limit.</p>
     */
    private void tryDispatch() {
        int current;
        int casAttempts = 0;

        do {
            current = inFlight.get();
            if (current >= maxParallel) {
                return; // parallelism limit reached
            }

            if (inFlight.compareAndSet(current, current + 1)) {
                break; // success, exit loop
            }

            casAttempts++;
        } while (casAttempts < MAX_CAS_ATTEMPTS_INFLIGHT);

        if (casAttempts >= MAX_CAS_ATTEMPTS_INFLIGHT) {
            return; // failed to increment inFlight after max attempts
        }

        executor.execute(() -> {
            try {
                do {
                    List<SolrInputDocument> batch = pollBatch();
                    if (batch.isEmpty()) {
                        break;
                    }

                    updateBatch(batch);
                } while (size() >= batchSize);
            } finally {
                inFlight.decrementAndGet();
            }
        });
    }

    /**
     * Adds a document to the buffer for batch updating.
     * 
     * <p>If there is space in the buffer, the document is added and may trigger
     * a batch update if enough documents have accumulated.</p>
     * 
     * <p>If the buffer is full, this method falls back to updating the document
     * immediately in Solr via {@link #updateSingle(SolrInputDocument)}. This
     * ensures that no documents are lost even under high load, and naturally
     * slows down the producer.</p>
     *
     * @param doc the Solr document to add
     */
    public void add(SolrInputDocument doc) {
        if (this.offer(doc)) {
            if (size() >= batchSize) {
                tryDispatch();
            }
        } else {
            LOGGER.fine("Ring buffer full, updating single document directly.");
            this.updateSingle(doc);
        }
    }

    /**
     * Flushes all documents currently in the buffer.
     * <p>
     * - Polls and updates all documents in the buffer in batches.
     * - Waits for all in-flight tasks to complete before returning.
     * </p>
     */
    public void flush() {
        List<SolrInputDocument> batch;
        while (!(batch = this.pollBatch()).isEmpty()) {
            this.updateBatch(batch);
        }

        // Wait for in-flight tasks to finish
        while (inFlight.get() > 0) {
            try {
                // tiny sleep to reduce busy-spin
                Thread.sleep(10); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Shuts down the updater.
     * <p>
     * - Flushes all remaining documents in the buffer.
     * - Shuts down the executor service and waits for termination.
     * </p>
     */
    public void shutdown() {
        this.flush();

        executor.shutdown();

        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOGGER.warning("Executor did not terminate within 60 seconds.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warning("Interrupted while waiting for executor termination.");
        }
    }
}
