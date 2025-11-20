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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Deklarace procesu je v shared/common/src/main/java/cz/incad/kramerius/processes/res/lp.st (processing_rebuild)
 */
public class ProcessingIndexRebuild {
    public static final Logger LOGGER = Logger.getLogger(ProcessingIndexCheck.class.getName());

    private static final int BATCH_SIZE = 10000;
    private static final int PRODUCER_THREADS = 1;
    private static final int CONSUMER_THREADS = Math.min(32, Runtime.getRuntime().availableProcessors() * 2);
    
    private static final BlockingQueue<Path> fileQueue = new LinkedBlockingQueue<>(BATCH_SIZE * (CONSUMER_THREADS / 2));
    private static final AtomicLong filesEnqueued = new AtomicLong(0);
    private static volatile boolean doneProducing = false;

    // Thread-local unmarshaller for safe concurrent usage
    private static final ThreadLocal<Unmarshaller> LOCAL_UNMARSHALLER = ThreadLocal.withInitial(() -> {
        try {
            JAXBContext context = JAXBContext.newInstance(DigitalObject.class);
            return context.createUnmarshaller();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init unmarshaller", e);
        }
    });

    public static void main(String[] args) throws IOException, SolrServerException {
        if (args.length>=1 && "REBUILDPROCESSING".equalsIgnoreCase(args[0])){
            LOGGER.info("Přebudování Processing indexu");
        } else {
            ProcessStarter.updateName("Přebudování Processing indexu");
        }
        Injector injector = Guice.createInjector(new SolrModule(), new RepoModule(), new NullStatisticsModule());
        final AkubraRepository akubraRepository = injector.getInstance(Key.get(AkubraRepository.class));

        long start = System.currentTimeMillis();
        akubraRepository.pi().deleteProcessingIndex();
        Path objectStoreRoot = 
            KConfiguration.getInstance().getConfiguration().getBoolean("legacyfs")
            ? Paths.get(KConfiguration.getInstance().getProperty("object_store_base"))
            : Paths.get(KConfiguration.getInstance().getProperty("objectStore.path"));
        
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
                                long count = filesEnqueued.incrementAndGet();
                                if (count % 10000 == 0) {
                                    LOGGER.info("Enqueued " + count + " files for processing so far...");
                                }
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
                List<String> batch = new ArrayList<>(BATCH_SIZE);
                Path file;
                while (!doneProducing || !fileQueue.isEmpty()) {
                    try {
                        file = fileQueue.poll(1, TimeUnit.SECONDS);
                        if (file == null) {
                            continue;
                        }

                        try (InputStream in = Files.newInputStream(file)) {
                            DigitalObject obj = (DigitalObject) LOCAL_UNMARSHALLER.get().unmarshal(in);

                            if (obj == null) {
                                LOGGER.severe("Failed to unmarshal object from file: " + file);
                                continue;
                            }

                            batch.add(obj.getPID());

                            if (batch.size() >= BATCH_SIZE) {
                                akubraRepository.pi().rebuildProcessingIndexBatch(new ArrayList<>(batch), null);
                                batch.clear();
                                LOGGER.info("Processed batch of " + BATCH_SIZE + " PIDs");
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
                        akubraRepository.pi().rebuildProcessingIndexBatch(batch, null);
                        LOGGER.info("Processed batch of " + batch.size() + " PIDs");
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error flushing remaining batch", e);
                    }
                }
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

        akubraRepository.pi().commit();
        akubraRepository.shutdown();
    }

    public static void rebuildProcessingIndex(AkubraRepository akubraRepository, DigitalObject digitalObject,Consumer<UpdateRequest> var2 ) {
        akubraRepository.pi().rebuildProcessingIndex(digitalObject.getPID(), var2);
    }

    public static void rebuildProcessingIndex(AkubraRepository akubraRepository, String pid, Consumer<UpdateRequest> var2) {
        akubraRepository.pi().rebuildProcessingIndex(pid, var2);
    }
}
