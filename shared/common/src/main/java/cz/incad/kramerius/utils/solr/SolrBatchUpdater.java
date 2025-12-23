package cz.incad.kramerius.utils.solr;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Basic implementation of AbstractSolrBatchUpdater without monitoring callbacks.
 */
public class SolrBatchUpdater extends AbstractSolrBatchUpdater {
    public static final Logger LOGGER = Logger.getLogger(SolrBatchUpdater.class.getName());

    /**
     * Creates a new SolrBatchUpdater.
     *
     * @param batchSize the maximum number of documents per batch sent to Solr
     * @param numParallel the maximum number of concurrent batch update threads
     * @param solrClient the SolrClient instance used for updates
     * @param collection the Solr collection to update
     */
    public SolrBatchUpdater(int batchSize, int numParallel, SolrClient solrClient, String collection) {
        super(batchSize, numParallel, solrClient, collection);
    }

    @Override
    protected void updateSingle(SolrInputDocument doc) {
        try {
            solrClient.add(this.collection, doc);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update single document", e);
        }
    }

    @Override
    protected void updateBatch(List<SolrInputDocument> docs) {
        try {
            solrClient.add(this.collection, docs);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update batch of " + docs.size() + " documents", e);

            for (SolrInputDocument doc : docs) {
                updateSingle(doc);
            }
        }
    }
}
