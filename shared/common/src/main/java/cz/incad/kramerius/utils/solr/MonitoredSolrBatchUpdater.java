package cz.incad.kramerius.utils.solr;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.BiConsumer;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Extension of AbstractSolrBatchUpdater that provides monitoring callbacks
 * for indexing operations.
 */
public class MonitoredSolrBatchUpdater extends AbstractSolrBatchUpdater {
    public static final Logger LOGGER = Logger.getLogger(MonitoredSolrBatchUpdater.class.getName());

    private final Consumer<SolrInputDocument> onIndexed;
    private final Consumer<List<SolrInputDocument>> onIndexedBatch;
    private final BiConsumer<SolrInputDocument, Exception> onFailed;

    /**
     * Creates a new MonitoredSolrBatchUpdater.
     *
     * @param batchSize the maximum number of documents per batch sent to Solr
     * @param numParallel the maximum number of concurrent batch update threads
     * @param solrClient the SolrClient instance used for updates
     * @param onIndexed callback invoked after a document is successfully indexed
     * @param onIndexedBatch callback invoked after a batch of documents is successfully indexed
     * @param onFailed callback invoked after a document fails to index
     */
    public MonitoredSolrBatchUpdater(
        int batchSize,
        int numParallel,
        SolrClient solrClient,
        Consumer<SolrInputDocument> onIndexed,
        Consumer<List<SolrInputDocument>> onIndexedBatch,
        BiConsumer<SolrInputDocument, Exception> onFailed
    ) {
        super(batchSize, numParallel, solrClient);
        this.onIndexed = onIndexed;
        this.onIndexedBatch = onIndexedBatch;
        this.onFailed = onFailed;
    }

    @Override
    protected void updateSingle(SolrInputDocument doc) {
        try {
            solrClient.add(doc);
            onIndexed.accept(doc);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update single document", e);
            onFailed.accept(doc, e);
        }
    }

    @Override
    protected void updateBatch(List<SolrInputDocument> docs) {
        try {
            solrClient.add(docs);
            onIndexedBatch.accept(docs);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update batch of " + docs.size() + " documents", e);

            for (SolrInputDocument doc : docs) {
                updateSingle(doc);
            }
        }
    }
}
