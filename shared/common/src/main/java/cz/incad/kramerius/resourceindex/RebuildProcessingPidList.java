package cz.incad.kramerius.resourceindex;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.qbizm.kramerius.imp.jaxb.DatastreamType;
import com.qbizm.kramerius.imp.jaxb.DigitalObject;
import cz.incad.kramerius.fedora.RepoModule;
import cz.incad.kramerius.fedora.om.RepositoryException;
import cz.incad.kramerius.fedora.om.impl.AkubraObject;
import cz.incad.kramerius.fedora.om.impl.AkubraUtils;
import cz.incad.kramerius.fedora.om.impl.RELSEXTSPARQLBuilder;
import cz.incad.kramerius.fedora.om.impl.RELSEXTSPARQLBuilderImpl;
import cz.incad.kramerius.processes.starter.ProcessStarter;
import cz.incad.kramerius.solr.SolrModule;
import cz.incad.kramerius.statistics.NullStatisticsModule;
import cz.incad.kramerius.utils.FedoraUtils;
import cz.incad.kramerius.utils.conf.KConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.xml.sax.SAXException;

import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Deklarace procesu je v shared/common/src/main/java/cz/incad/kramerius/processes/res/lp.st (processing_rebuild_for_object)
 */
public class RebuildProcessingPidList {
    public static final Logger LOGGER = Logger.getLogger(RebuildProcessingPidList.class.getName());

    private static final String OBJECT_STORE_PATTERN = "##/##/##";
    private static final Path OBJECT_STORE_PATH = Paths.get("/mnt/akubra/objectStore");
    private static final String SOLR_HOST = "http://127.0.0.1:8983/solr/processing";
    private static final Pattern PID_PATTERN = Pattern.compile("uuid:[a-f0-9]{8}\\-([a-f0-9]{4}\\-){3}[a-f0-9]{12}");

    private final Unmarshaller unmarshaller;
    private final ProcessingIndexFeeder feeder;

    private RebuildProcessingPidList() {
        this.unmarshaller = initUnmarshaller();
        this.feeder =  new ProcessingIndexFeeder(new HttpSolrClient(SOLR_HOST));
//        Injector injector = Guice.createInjector(new SolrModule(), new ResourceIndexModule(), new RepoModule(), new NullStatisticsModule());
//        this.feeder = injector.getInstance(ProcessingIndexFeeder.class);
    }

    /**
     * args[0] - authToken
     * args[1] - pid
     */
    public static void main(String[] args) throws IOException, SolrServerException, RepositoryException {
        RebuildProcessingPidList rebuildProcessingPidList = new RebuildProcessingPidList();
        for (String filename : args) {
            File file = new File(filename);
            if (file.exists()) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                List<String> pids = bufferedReader.lines()
                        .filter(pid -> PID_PATTERN.matcher(pid).matches())
                        .collect(Collectors.toList());
                rebuildProcessingPidList.rebuildProcessingIndexFromPidList(pids);
            }
        }
    }

    private Unmarshaller initUnmarshaller() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(DigitalObject.class);
            return jaxbContext.createUnmarshaller();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Cannot init JAXB", e);
            throw new RuntimeException(e);
        }
    }

    private void rebuildProcessingIndexFromPidList(List<String> pids) throws IOException {
        for (String pid : pids) {
            this.rebuildProcessingIndexFromFoxml(pid);
        }
        try {
            this.feeder.commit();
            LOGGER.info("CALLED PROCESSING INDEX COMMIT");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildProcessingIndexFromFoxml(String pid) throws IOException {
        LOGGER.log(Level.INFO, "Updating processing index from FOXML of " + pid);
        File foxmlFile = findFoxmlFile(pid);
        LOGGER.log(Level.INFO, "FOXML file: " + foxmlFile.getAbsolutePath());
        if (!foxmlFile.exists()) {
            LOGGER.severe("File doesn't exist: " + foxmlFile.getAbsolutePath());
        }
        if (!foxmlFile.canRead()) {
            LOGGER.severe("File can't be read: " + foxmlFile.getAbsolutePath());
        }
        try {
            FileInputStream inputStream = new FileInputStream(foxmlFile);
            DigitalObject digitalObject = createDigitalObject(inputStream);
//            feeder.deleteByPid(pid); //smazat vsechny existujici vazby z objektu, ALE netyka se tech, co na objekt vedou (ty ted neprebudovavame)
//            rebuildProcessingIndex(feeder, digitalObject);
            rebuildProcessingIndex(feeder, digitalObject);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error processing file: " + foxmlFile.getAbsolutePath(), ex);
        }
    }

    private File findFoxmlFile(String pid) {
        try {
            if (!pid.toLowerCase().startsWith("uuid:")) { //this is already checked at API endpoint level, here it's just to make sure if this class was to be called from somewhere else
                throw new IllegalArgumentException("invalid pid format");
            }
            String objectId = "info:fedora/" + pid.toLowerCase(); //e.g. info:fedora/uuid:912509d3-2764-4be5-9e0a-366cbacabfef
            //System.out.println(objectId);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(objectId.getBytes("UTF-8"));
            String objectIdHash = DatatypeConverter.printHexBinary(md.digest()); //e.g. 7C2BDE15DDDFA29123823CB7A86BFD86
            //System.out.println(objectIdHash);
//            String objectPattern = KConfiguration.getInstance().getProperty("objectStore.pattern"); //e.g. ##/##/##
            //System.out.println(objectPattern);
            String pathSegementsFromPid = PathSegmentExtractor.extractPathSegements(objectIdHash.toLowerCase(), OBJECT_STORE_PATTERN); //e.g. 7c/2b/de
            //System.out.println(pathSegementsFromPid);
            String foxmlPath = pathSegementsFromPid + "/info%3Afedora%2Fuuid%3A" + pid.substring("uuid:".length()); //e.g. 7c/2b/de/info%3Afedora%2Fuuid%3A912509d3-2764-4be5-9e0a-366cbacabfef
            //System.out.println(foxmlPath);
//            File objectStoreRoot = new File(KConfiguration.getInstance().getProperty("objectStore.path")); //e.g. /home/tomcat/kramerius-akubra/akubra-data/objectStore
            File objectStoreRoot = OBJECT_STORE_PATH.toFile();
            return new File(objectStoreRoot, foxmlPath); //e.g. /home/tomcat/kramerius-akubra/akubra-data/objectStore/
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private DigitalObject createDigitalObject(InputStream inputStream) {
        try {
            return (DigitalObject) unmarshaller.unmarshal(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildProcessingIndex(ProcessingIndexFeeder feeder, DigitalObject digitalObject) throws RepositoryException {
        try {
            List<DatastreamType> datastreamList = digitalObject.getDatastream();
            for (DatastreamType datastreamType : datastreamList) {
                if (FedoraUtils.RELS_EXT_STREAM.equals(datastreamType.getID())) {
                    InputStream streamContent = AkubraUtils.getStreamContent(AkubraUtils.getLastStreamVersion(datastreamType), null);
                    AkubraObject akubraObject = new AkubraObject(null, digitalObject.getPID(), digitalObject, feeder);
                    rebuildProcessingIndexImpl(akubraObject, streamContent);
                }
            }
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    private void rebuildProcessingIndexImpl(AkubraObject akubraObject, InputStream content) throws RepositoryException {
        try {
            String s = IOUtils.toString(content, "UTF-8");
            RELSEXTSPARQLBuilder sparqlBuilder = new RELSEXTSPARQLBuilderImpl();
            sparqlBuilder.sparqlProps(s.trim(), (object, localName) -> {
                akubraObject.processRELSEXTRelationAndFeedProcessingIndex(object, localName);
                return object;
            });
            LOGGER.info("Processed " + akubraObject.getPid());
        } catch (IOException e) {
            throw new RepositoryException(e);
        } catch (SAXException e) {
            throw new RepositoryException(e);
        } catch (ParserConfigurationException e) {
            throw new RepositoryException(e);
        } finally {
        }
    }

    /**
     * @see org.fcrepo.server.storage.lowlevel.akubra.HashPathIdMapper
     */
    public static class PathSegmentExtractor {
        public static String extractPathSegements(String string, String objectPattern) {
            if (!objectPattern.matches("#+(\\/#+)*")) {
                throw new RuntimeException(String.format("unsupported object pattern: %s", objectPattern));
            }
            if (objectPattern.replaceAll("\\/", "").length() > string.length()) {
                throw new RuntimeException(String.format("string too short for the pattern: %s, string: %s", objectPattern, string));
            }
            StringBuilder builder = new StringBuilder();
            String[] placeholders = objectPattern.split("\\/");
            int startingPosition = 0;
            for (int i = 0; i < placeholders.length; i++) {
                String placeholder = placeholders[i];
                builder.append(string, startingPosition, startingPosition + placeholder.length());
                startingPosition += placeholder.length();
                if (placeholders.length != 1 && i != placeholders.length - 1) {
                    builder.append('/');
                }
            }
            return builder.toString();
        }
    }
}
