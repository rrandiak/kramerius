package cz.incad.kramerius.resourceindex;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import cz.incad.kramerius.fedora.RepoModule;
import cz.incad.kramerius.processes.starter.ProcessStarter;
import cz.incad.kramerius.solr.SolrModule;
import cz.incad.kramerius.statistics.NullStatisticsModule;
import cz.incad.kramerius.utils.FedoraUtils;
import cz.incad.kramerius.utils.conf.KConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.ceskaexpedice.akubra.AkubraRepository;
import org.ceskaexpedice.akubra.RepositoryException;
import org.ceskaexpedice.akubra.processingindex.ProcessingIndex;
import org.ceskaexpedice.fedoramodel.DatastreamType;
import org.ceskaexpedice.fedoramodel.DigitalObject;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Deklarace procesu je v shared/common/src/main/java/cz/incad/kramerius/processes/res/lp.st (processing_rebuild)
 */
public class ProcessingIndexRebuild {
    public static final Logger LOGGER = Logger.getLogger(ProcessingIndexCheck.class.getName());

    private static final int BATCH_SIZE = 10000;
    private static final int PRODUCER_THREADS = 1;
    private static final int CONSUMER_THREADS = Runtime.getRuntime().availableProcessors() * 4;

    private static final BlockingQueue<Path> fileQueue = new ArrayBlockingQueue<>(BATCH_SIZE * 2);
    private static volatile boolean doneProducing = false;
    private static final AtomicLong pidsProcessed = new AtomicLong(0);

    public static void main(String[] args) throws IOException, SolrServerException {
        if (args.length>=1 && "REBUILDPROCESSING".equalsIgnoreCase(args[0])){
            LOGGER.info("Přebudování Processing indexu");
        } else {
            ProcessStarter.updateName("Přebudování Processing indexu");
        }

        final AkubraRepository akubraRepository = getAkubraRepository();
        akubraRepository.pi().deleteProcessingIndex();
        akubraRepository.pi().commit();
        akubraRepository.shutdown();

        long start = System.currentTimeMillis();
        Path objectStoreRoot = 
            KConfiguration.getInstance().getConfiguration().getBoolean("legacyfs")
            ? Paths.get(KConfiguration.getInstance().getProperty("object_store_base"))
            : Paths.get(KConfiguration.getInstance().getProperty("objectStore.path"));
        
        LOGGER.info(
            "Starting rebuild processing process with:"
            + "\n    Object store root path: " + objectStoreRoot.toString()
            + "\n    Number of producer (file visitor) threads: " + PRODUCER_THREADS
            + "\n    Number of consumer (unmarshalling and indexing) threads: " + CONSUMER_THREADS
            + "\n    Index batch size: " + BATCH_SIZE
        );
        
        // Producer: walk file tree and submit tasks
        ExecutorService producer = Executors.newFixedThreadPool(PRODUCER_THREADS);
        // Files.walkFileTree() is used because it does not store any Paths in memory,
        // which makes it a more efficient solution to the problem compared to Files.walk().
        producer.submit(() -> {
            try {
                Files.walkFileTree(objectStoreRoot,
                    Collections.singleton(FileVisitOption.FOLLOW_LINKS),
                    Integer.MAX_VALUE,
                    new FileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (!Files.isRegularFile(file)) {
                                return FileVisitResult.CONTINUE;
                            }

                            try {
                                fileQueue.put(file);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Producer thread interrupted", e);
                            }

                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                            LOGGER.log(Level.SEVERE, "Error processing file: " + file.toString(), exc);

                            // This will allow the execution to continue uninterrupted,
                            // even in the event of encountering permission errors.
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            if (exc != null) {
                                LOGGER.log(Level.SEVERE, "Error searching directory : " + dir.toString(), exc);
                            }

                            // This will allow the execution to continue uninterrupted,
                            // even in the event of encountering permission errors.
                            return FileVisitResult.CONTINUE;
                        }
                    });
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                doneProducing = true;
                LOGGER.info("Done producing PIDs");
            }
        });

        // Consumers: batch processing
        ExecutorService consumers = Executors.newFixedThreadPool(CONSUMER_THREADS);
        for (int i = 0; i < CONSUMER_THREADS; i++) {
            consumers.submit(() -> {
                final AkubraRepository consumerRepo = getAkubraRepository();
                List<String> batch = new ArrayList<>(BATCH_SIZE);
                Path file;
                while (!doneProducing || !fileQueue.isEmpty()) {
                    try {
                        file = fileQueue.poll(1, TimeUnit.SECONDS);
                        if (file == null) {
                            continue;
                        }

                        try {
                            String filename = file.getFileName().toString();

                            if (!filename.startsWith("info%3Afedora%2Fuuid%3A")) {
                                LOGGER.warning("File name does not start with expected prefix: " + filename);
                                continue;
                            }

                            String pid = "uuid:" + filename.substring("info%3Afedora%2Fuuid%3A".length());

                            batch.add(pid);

                            if (batch.size() >= BATCH_SIZE) {
                                consumerRepo.pi().rebuildProcessingIndexBatch(batch, null);
                                LOGGER.info("Processed " + pidsProcessed.addAndGet(batch.size()) + " PIDs so far");
                                batch.clear();
                            }
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Error reading file: " + file, e);
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing batch", e);
                    }
                }
                // Flush remaining batch
                if (!batch.isEmpty()) {
                    try {
                        consumerRepo.pi().rebuildProcessingIndexBatch(batch, null);
                        LOGGER.info("Processed batch of " + batch.size() + " PIDs");
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error flushing remaining batch", e);
                    }
                }

                consumerRepo.pi().commit();
                consumerRepo.shutdown();
            });
        }

        // Shutdown executors
        producer.shutdown();
        try {
            if (!producer.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                LOGGER.severe("Producer did not terminate.");
            }
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Producer interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        }

        consumers.shutdown();
        try {
            if (!consumers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                LOGGER.severe("Consumers did not terminate.");
            }
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Consumers interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        }

        LOGGER.info("Finished tree walk in " + (System.currentTimeMillis() - start) + " ms");
    }

    public static AkubraRepository getAkubraRepository() {
        Injector injector = Guice.createInjector(new SolrModule(), new RepoModule(), new NullStatisticsModule());
        return injector.getInstance(Key.get(AkubraRepository.class));
    }

    public static void rebuildProcessingIndex(AkubraRepository akubraRepository, DigitalObject digitalObject,Consumer<UpdateRequest> var2 ) {
        akubraRepository.pi().rebuildProcessingIndex(digitalObject.getPID(), var2);
    }

    public static void rebuildProcessingIndex(AkubraRepository akubraRepository, String pid, Consumer<UpdateRequest> var2) {
        akubraRepository.pi().rebuildProcessingIndex(pid, var2);
    }
}
