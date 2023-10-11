package cz.incad.kramerius;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import cz.incad.kramerius.ProcessHelper.PidsOfDescendantsProducer;
import cz.incad.kramerius.fedora.RepoModule;
import cz.incad.kramerius.fedora.om.RepositoryException;
import cz.incad.kramerius.fedora.om.impl.AkubraDOManager;
import cz.incad.kramerius.impl.SolrAccessImplNewIndex;
import cz.incad.kramerius.processes.new_api.ProcessScheduler;
import cz.incad.kramerius.processes.starter.ProcessStarter;
import cz.incad.kramerius.processes.utils.ProcessUtils;
import cz.incad.kramerius.repository.KrameriusRepositoryApi;
import cz.incad.kramerius.repository.KrameriusRepositoryApiImpl;
import cz.incad.kramerius.resourceindex.IResourceIndex;
import cz.incad.kramerius.resourceindex.ResourceIndexException;
import cz.incad.kramerius.resourceindex.ResourceIndexModule;
import cz.incad.kramerius.solr.SolrModule;
import cz.incad.kramerius.statistics.NullStatisticsModule;
import cz.incad.kramerius.utils.Dom4jUtils;
import cz.kramerius.adapters.ProcessingIndex;
import cz.kramerius.searchIndex.indexer.SolrConfig;
import cz.kramerius.searchIndex.indexer.SolrIndexAccess;
import cz.kramerius.adapters.impl.krameriusNewApi.ProcessingIndexImplByKrameriusNewApis;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Prepsana logika z ParametrizedLabelSetDNNTFlag a ParametrizedLabelUnsetDNNTFlag, bez zpracovani CSV
 * Deklarace procesu je v shared/common/src/main/java/cz/incad/kramerius/processes/res/lp.st (add_license, remove_license)
 *
 * @see cz.incad.kramerius.workers.DNNTLabelWorker
 */
public class SetLicenseProcess {
    public enum Action {
        ADD, REMOVE
    }

    private static final Logger LOGGER = Logger.getLogger(SetLicenseProcess.class.getName());

    //TODO: properly cleanup what belongs here and what to LicenseHelper

    private static String RELS_EXT_RELATION_LICENSE = "license";
    private static String RELS_EXT_RELATION_CONTAINS_LICENSE = "containsLicense";
    private static String[] RELS_EXT_RELATION_LICENSE_DEPRECATED = new String[]{
            "licenses",
            "licence", "licences",
            "dnnt-label", "dnnt-labels"
    };
    private static String[] RELS_EXT_RELATION_CONTAINS_LICENSE_DEPRECATED = new String[]{
            "containsLicenses",
            "containsLicence", "containsLicences",
            "contains-license", "contains-licenses",
            "contains-licence", "contains-licenses",
            "contains-dnnt-label", "contains-dnnt-labels",
    };

    private static String SOLR_FIELD_LICENSES = "licenses";
    private static String SOLR_FIELD_CONTAINS_LICENSES = "contains_licenses";
    private static String SOLR_FIELD_LICENSES_OF_ANCESTORS = "licenses_of_ancestors";

    /**
     * args[0] - action (ADD/REMOVE), from lp.st process/parameters
     * args[1] - authToken
     * args[2] - target (pid:uuid:123, or pidlist:uuid:123;uuid:345;uuid:789, or pidlist_file:/home/kramerius/.kramerius/import-dnnt/grafiky.txt
     * In case of pidlist pids must be separated with ';'. Convenient separator ',' won't work due to way how params are stored in database and transferred to process.
     * <p>
     * args[3] - licence ('dnnt', 'dnnto', 'public_domain', etc.)
     */
    public static void main(String[] args) throws IOException, SolrServerException, RepositoryException, ResourceIndexException {
        //args
        /*LOGGER.info("args: " + Arrays.asList(args));
        for (String arg : args) {
            System.out.println(arg);
        }*/
        if (args.length < 4) {
            throw new RuntimeException("Not enough arguments.");
        }
        int argsIndex = 0;
        //params from lp.st
        Action action = Action.valueOf(args[argsIndex++]);
        //token for keeping possible following processes in same batch
        String authToken = args[argsIndex++]; //auth token always second, but still suboptimal solution, best would be if it was outside the scope of this as if ProcessHelper.scheduleProcess() similarly to changing name (ProcessStarter)
        //process params
        String license = args[argsIndex++];
        String target = args[argsIndex++];


        Injector injector = Guice.createInjector(new SolrModule(), new ResourceIndexModule(), new RepoModule(), new NullStatisticsModule(), new ResourceIndexModule());
        KrameriusRepositoryApi repository = injector.getInstance(Key.get(KrameriusRepositoryApiImpl.class)); //FIXME: hardcoded implementation

        SolrAccess searchIndex = injector.getInstance(Key.get(SolrAccessImplNewIndex.class)); //FIXME: hardcoded implementation
        SolrIndexAccess indexerAccess = new SolrIndexAccess(new SolrConfig());

        // IResourceIndex resourceIndex = new ResourceIndexImplByKrameriusNewApis(repository, ProcessUtils.getCoreBaseUrl());
        ProcessingIndex processingIndex = new ProcessingIndexImplByKrameriusNewApis(repository, ProcessUtils.getCoreBaseUrl());

        switch (action) {
            case ADD:
                ProcessStarter.updateName(String.format("Přidání licence '%s' pro %s", license, target));
                for (String pid : extractPids(target)) {
                    try {
                        addLicense(license, pid, repository, processingIndex, searchIndex, indexerAccess);
                    } catch (Exception ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                        LOGGER.log(Level.SEVERE, String.format("Skipping object %s", pid));
                    }
                }
                break;
            case REMOVE:
                ProcessStarter.updateName(String.format("Odebrání licence '%s' pro %s", license, target));
                for (String pid : extractPids(target)) {
                    try {
                        removeLicense(license, pid, repository, processingIndex, searchIndex, indexerAccess, authToken);
                    } catch (Exception ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                        LOGGER.log(Level.SEVERE, String.format("Skipping object %s", pid));
                    }
                }
                break;
        }
    }

    private static List<String> extractPids(String target) {
        if (target.startsWith("pid:")) {
            String pid = target.substring("pid:".length());
            List<String> result = new ArrayList<>();
            result.add(pid);
            return result;
        } else if (target.startsWith("pidlist:")) {
            List<String> pids = Arrays.stream(target.substring("pidlist:".length()).split(";")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
            return pids;
        } else if (target.startsWith("pidlist_file:")) {
            String filePath = target.substring("pidlist_file:".length());
            File file = new File(filePath);
            if (file.exists()) {
                try {
                    return IOUtils.readLines(new FileInputStream(file), Charset.forName("UTF-8"));
                } catch (IOException e) {
                    throw new RuntimeException("IOException " + e.getMessage());
                }
            } else {
                throw new RuntimeException("file " + file.getAbsolutePath() + " doesnt exist ");
            }
        } else {
            throw new RuntimeException("invalid target " + target);
        }
    }

    private static void addLicense(String license, String targetPid, KrameriusRepositoryApi repository, ProcessingIndex processingIndex, SolrAccess searchIndex, SolrIndexAccess indexerAccess) throws RepositoryException, IOException, ResourceIndexException {
        LOGGER.info(String.format("Adding license '%s' to %s", license, targetPid));

        //1. Do rels-ext ciloveho objektu se doplni license=L, pokud uz tam neni. Nejprve se ale normalizuji stare zapisy licenci (dnnt-label=L => license=L)
        LOGGER.info("updating RELS-EXT record of the target object " + targetPid);
        addRelsExtRelationAfterNormalization(targetPid, RELS_EXT_RELATION_LICENSE, RELS_EXT_RELATION_LICENSE_DEPRECATED, license, repository);

        //2. Do rels-ext (vlastnich) predku se doplni containsLicense=L, pokud uz tam neni
        LOGGER.info("updating RELS-EXT record of all (own) ancestors of the target object " + targetPid);
        List<String> pidsOfAncestors = getPidsOfOwnAncestors(targetPid, processingIndex);
        for (String ancestorPid : pidsOfAncestors) {
            addRelsExtRelationAfterNormalization(ancestorPid, RELS_EXT_RELATION_CONTAINS_LICENSE, RELS_EXT_RELATION_CONTAINS_LICENSE_DEPRECATED, license, repository);
        }

        //3. Aktualizuji se indexy vsech (vlastnich) predku (prida se contains_licenses=L) atomic updatem
        LOGGER.info("updating search index for all (own) ancestors");
        indexerAccess.addSingleFieldValueForMultipleObjects(pidsOfAncestors, SOLR_FIELD_CONTAINS_LICENSES, license, false);

        //4. Aktualizuje se index ciloveho objektu (prida se licenses=L) atomic updatem
        LOGGER.info("updating search index for the target object");
        List<String> targetPidOnly = new ArrayList<>();
        targetPidOnly.add(targetPid);
        indexerAccess.addSingleFieldValueForMultipleObjects(targetPidOnly, SOLR_FIELD_LICENSES, license, false);

        //5. Aktualizuji se indexy vsech (vlastnich i nevlastnich) potomku (prida se licenses_of_ancestors=L) atomic updaty po davkach (muzou to byt az stovky tisic objektu)
        LOGGER.info("updating search index for all (own) descendants of target object");
        PidsOfDescendantsProducer iterator = new PidsOfDescendantsProducer(targetPid, searchIndex, false);
        while (iterator.hasNext()) {
            List<String> pids = iterator.next();
            indexerAccess.addSingleFieldValueForMultipleObjects(pids, SOLR_FIELD_LICENSES_OF_ANCESTORS, license, false);
            LOGGER.info(String.format("Indexed: %d/%d", iterator.getReturned(), iterator.getTotal()));
        }

        //commit changes in index
        try {
            indexerAccess.commit();
        } catch (IOException | SolrServerException e) {
            e.printStackTrace();
            throw new RuntimeException((e));
        }
    }

    private static List<String> getPidsOfOwnAncestors(String targetPid, ProcessingIndex processingIndex) throws ResourceIndexException {
        List<String> result = new ArrayList<>();
        String pidOfCurrentNode = targetPid;
        String pidOfCurrentNodesOwnParent;
        while ((pidOfCurrentNodesOwnParent = processingIndex.getPidsOfParents(pidOfCurrentNode).getFirst()) != null) {
            result.add(pidOfCurrentNodesOwnParent);
            pidOfCurrentNode = pidOfCurrentNodesOwnParent;
        }
        return result;
    }

   /* private static List<String> getPidsOfAllAncestors(String targetPid, SolrAccess searchIndex) throws IOException {
        Set<String> result = new HashSet<>();
        ObjectPidsPath[] pidPaths = searchIndex.getPidPaths(targetPid);
        for (ObjectPidsPath pidPath : pidPaths) {
            String[] pathFromRootToLeaf = pidPath.getPathFromRootToLeaf();
            for (String pathPid : pathFromRootToLeaf) {
                if (!targetPid.equals(pathPid)) {
                    result.add(pathPid);
                }
            }
        }
        return new ArrayList<>(result);
    }*/

    private static boolean addRelsExtRelationAfterNormalization(String pid, String relationName, String[] wrongRelationNames, String value, KrameriusRepositoryApi repository) throws RepositoryException, IOException {
        Lock writeLock = AkubraDOManager.getWriteLock(pid);
        try {
            if (!repository.isRelsExtAvailable(pid)) {
                throw new RepositoryException("RDF record (datastream RELS-EXT) not found for " + pid);
            }
            Document relsExt = repository.getRelsExt(pid, true);
            Element rootEl = (Element) Dom4jUtils.buildXpath("/rdf:RDF/rdf:Description").selectSingleNode(relsExt);
            boolean relsExtNeedsToBeUpdated = false;

            //normalize relations with deprecated/incorrect name, possibly including relation we want to add
            relsExtNeedsToBeUpdated |= LicenseHelper.normalizeIncorrectRelationNotation(wrongRelationNames, relationName, rootEl, pid);

            //add new relation if not there already
            List<Node> relationEls = Dom4jUtils.buildXpath("rel:" + relationName).selectNodes(rootEl);
            boolean relationFound = false;
            for (Node relationEl : relationEls) {
                String content = relationEl.getText();
                if (content.equals(value)) {
                    relationFound = true;
                }
            }
            if (!relationFound) {
                Element newRelationEl = rootEl.addElement(relationName, Dom4jUtils.getNamespaceUri("rel"));
                newRelationEl.addText(value);
                relsExtNeedsToBeUpdated = true;
                LOGGER.info(String.format("adding relation '%s' into RELS-EXT of %s", relationName, pid));
            } else {
                LOGGER.info(String.format("relation '%s' already found in RELS-EXT of %s", relationName, pid));
            }

            //update RELS-EXT in repository if there was a change
            if (relsExtNeedsToBeUpdated) {
                //System.out.println(Dom4jUtils.docToPrettyString(relsExt));
                repository.updateRelsExt(pid, relsExt);
                LOGGER.info(String.format("RELS-EXT of %s has been updated", pid));
            }
            return relsExtNeedsToBeUpdated;
        } finally {
            writeLock.unlock();
        }
    }

    private static void removeLicense(String license, String targetPid, KrameriusRepositoryApi repository, ProcessingIndex processingIndex, SolrAccess searchIndex, SolrIndexAccess indexerAccess, String authToken) throws RepositoryException, IOException, ResourceIndexException {
        LOGGER.info(String.format("Removing license '%s' from %s", license, targetPid));

        //1. Z rels-ext ciloveho objektu se odebere license=L, pokud tam je. Nejprve se ale normalizuji stare zapisy licenci (dnnt-label=L => license=L)
        LOGGER.info("updating RELS-EXT record of the target object " + targetPid);
        LicenseHelper.removeRelsExtRelationAfterNormalization(targetPid, RELS_EXT_RELATION_LICENSE, RELS_EXT_RELATION_LICENSE_DEPRECATED, license, repository);

        //2. Z rels-ext vsech (vlastnich) predku se odebere containsLicence=L, pokud tam je.
        //A pokud neexistuje jiny zdroj pro licenci (jiny potomek predka, ktery ma rels-ext:containsLicense kvuli jineho objektu, nez targetPid)
        //Takovy objekt (jiny zdroj) muze byt kdekoliv, treba ve stromu objektu targetPid
        LOGGER.info("updating RELS-EXT record of all (own) ancestors (without another source of license) of the target object " + targetPid);
        List<String> pidsOfAncestorsWithoutAnotherSourceOfLicense = getPidsOfOwnAncestorsWithoutAnotherSourceOfLicense(targetPid, repository, processingIndex, license);
        for (String ancestorPid : pidsOfAncestorsWithoutAnotherSourceOfLicense) {
            LicenseHelper.removeRelsExtRelationAfterNormalization(ancestorPid, RELS_EXT_RELATION_CONTAINS_LICENSE, RELS_EXT_RELATION_CONTAINS_LICENSE_DEPRECATED, license, repository);
        }

        //3. Aktualizuje se index predku, kteri nemaji jiny zdroj licence (odebere se contains_licenses=L) atomic updatem
        LOGGER.info("updating search index of all (own) ancestors without another source of license");
        indexerAccess.removeSingleFieldValueFromMultipleObjects(pidsOfAncestorsWithoutAnotherSourceOfLicense, SOLR_FIELD_CONTAINS_LICENSES, license, false);

        //4. Aktualizuje se index ciloveho objektu (odebere se licenses=L) atomic updatem
        LOGGER.info("updating search index for the target object");
        List<String> targetPidOnly = new ArrayList<>();
        targetPidOnly.add(targetPid);
        indexerAccess.removeSingleFieldValueFromMultipleObjects(targetPidOnly, SOLR_FIELD_LICENSES, license, false);

        //5. Pokud uz zadny z (vlastnich) predku ciloveho objektu nevlastni licenci, aktualizuji se indexy potomku ciloveho objektu (v opacnem pripade to neni treba)
        if (!hasAncestorThatOwnsLicense(targetPid, license, processingIndex, repository)) {
            //5a. Aktualizuji se indexy vsech (vlastnich) potomku (odebere se licenses_of_ancestors=L) atomic updaty po davkach (muzou to byt az stovky tisic objektu)
            LOGGER.info("updating search index for all (own) descendants of target object");
            PidsOfDescendantsProducer descendantsIterator = new PidsOfDescendantsProducer(targetPid, searchIndex, true);
            while (descendantsIterator.hasNext()) {
                List<String> pids = descendantsIterator.next();
                indexerAccess.removeSingleFieldValueFromMultipleObjects(pids, SOLR_FIELD_LICENSES_OF_ANCESTORS, license, false);
                LOGGER.info(String.format("Indexed: %d/%d", descendantsIterator.getReturned(), descendantsIterator.getTotal()));
            }
            //5b. Vsem potomkum ciloveho objektu, ktere take vlastni licenci, budou aktualizovany licence jejich potomku (prida se licenses_of_ancestors=L), protoze byly nepravem odebrany v kroku 5a.
            List<String> pidsOfDescendantsOfTargetOwningLicence = getDescendantsOwningLicense(targetPid, license, repository, processingIndex);
            for (String pid : pidsOfDescendantsOfTargetOwningLicence) {
                PidsOfDescendantsProducer iterator = new PidsOfDescendantsProducer(pid, searchIndex, true);
                while (iterator.hasNext()) {
                    List<String> pids = iterator.next();
                    indexerAccess.addSingleFieldValueForMultipleObjects(pids, SOLR_FIELD_LICENSES_OF_ANCESTORS, license, false);
                    LOGGER.info(String.format("Indexed: %d/%d", iterator.getReturned(), iterator.getTotal()));
                }
            }
        }

        //6. pokud ma target nevlastni deti (tj. je sbirka, clanek, nebo obrazek), synchronizuje se jejich index
        List<String> fosterChildren = processingIndex.getPidsOfChildren(targetPid).getSecond();
        if (fosterChildren != null && !fosterChildren.isEmpty()) {
            //6a. vsem potomkum (primi/neprimi, vlastni/nevlastni) budou aktualizovany licence (odebere se licenses_of_ancestors=L)
            PidsOfDescendantsProducer allDescendantsIterator = new PidsOfDescendantsProducer(targetPid, searchIndex, false);
            List<String> pids = allDescendantsIterator.next();
            indexerAccess.removeSingleFieldValueFromMultipleObjects(pids, SOLR_FIELD_LICENSES_OF_ANCESTORS, license, false);
            LOGGER.info(String.format("Indexed: %d/%d", allDescendantsIterator.getReturned(), allDescendantsIterator.getTotal()));

            //6b. naplanuje se reindexace target, aby byly opraveny pripadne chyby zanasene v bode 6a
            //nekteri potomci mohli mit narok na licenci z jineho zdroje ve svem strome, coz nelze u odebirani licence nevlastniho predka efektivne zjistit
            ProcessScheduler.scheduleIndexation(targetPid, null, true, authToken);
        }
        //commit changes in index
        try {
            indexerAccess.commit();
        } catch (IOException | SolrServerException e) {
            e.printStackTrace();
            throw new RuntimeException((e));
        }
    }

    private static boolean hasAncestorThatOwnsLicense(String pid, String license, ProcessingIndex processingIndex, KrameriusRepositoryApi repository) throws ResourceIndexException, RepositoryException, IOException {
        String currentPid = pid;
        String parentPid;
        while ((parentPid = processingIndex.getPidsOfParents(currentPid).getFirst()) != null) {
            if (LicenseHelper.ownsLicenseByRelsExt(parentPid, license, repository)) {
                return true;
            }
            currentPid = parentPid;
        }
        return false;
    }

    private static List<String> getDescendantsOwningLicense(String targetPid, String license, KrameriusRepositoryApi repository, ProcessingIndex processingIndex) throws ResourceIndexException, RepositoryException, IOException {
        List<String> result = new ArrayList<>();
        if (LicenseHelper.containsLicenseByRelsExt(targetPid, license, repository)) { //makes sense only if object itself contains license
            List<String> pidsOfOwnChildren = processingIndex.getPidsOfChildren(targetPid).getFirst();
            for (String childPid : pidsOfOwnChildren) {
                if (LicenseHelper.ownsLicenseByRelsExt(childPid, license, repository)) {
                    result.add(childPid);
                }
                if (LicenseHelper.containsLicenseByRelsExt(childPid, license, repository)) {
                    result.addAll(getDescendantsOwningLicense(childPid, license, repository, processingIndex));
                }
            }
        }
        return result;
    }

    /**
     * Returns list of pids of own ancestors of an object (@param pid), that don't have another source of license but this object (@param pid)
     * Object is never source of license for itself. Meaning that if it has rels-ext:license, but no rels-ext:containsLicense, it is considered not having source of license.
     */
    private static List<String> getPidsOfOwnAncestorsWithoutAnotherSourceOfLicense(String pid, KrameriusRepositoryApi repository, ProcessingIndex processingIndex, String license) throws IOException, ResourceIndexException, RepositoryException {
        List<String> result = new ArrayList<>();
        String pidOfChild = pid;
        String pidOfParent;
        while ((pidOfParent = processingIndex.getPidsOfParents(pidOfChild).getFirst()) != null) {
            String pidToBeIgnored = pidOfChild.equals(pid) ? null : pidOfChild; //only grandparent of original pid can be ignored, because it has been already analyzed in this loop, but not the original pid
            boolean hasAnotherSourceOfLicense = LicenseHelper.hasAnotherSourceOfLicense(pidOfParent, pid, pidToBeIgnored, license, repository, processingIndex);
            boolean ownsLicense = LicenseHelper.ownsLicenseByRelsExt(pidOfParent, license, repository);
            if (!hasAnotherSourceOfLicense) { //add this to the list
                result.add(pidOfParent);
            }
            if (hasAnotherSourceOfLicense) { //this has source for itself and thus for it's ancestors
                break;
            }
            if (ownsLicense) { //is itself source for it's ancestors (but not necessarily for itself, meaning it doesn't necessarily have rels-ext:containsLicense)
                break;
            }
            pidOfChild = pidOfParent;
        }
        return result;
    }


    //private static List<String> getPidsOfAllAncestorsThatDontHaveLicenceFromDifferentDescendant(String targetPid, SolrAccess searchIndex, String license) throws IOException {
    //    List<String> ancestorsAll = getPidsOfAllAncestors(targetPid, searchIndex);
    //    List<String> ancestorsWithoutLicenceFromAnotherDescendant = new ArrayList<>();
    //    for (String ancestorPid : ancestorsAll) {
    //        //hledaji se objekty, jejichz pid_path obsahuje ancestorPid, ale ne targetPid. Takze jiny zdroj stejne licence nekde ve strome ancestra, mimo strom od targeta
    //        //pokud nejsou, priznak containsLicense/contains_licenses muze byt z ancestra odstranen
    //        //jedine kdyby byl treba rocnik R1, co ma v rels-ext containsLicense=L a obsahuje nektere issue Ix, coma ma taky containsLicense=L, tak to se pri odstranovani L z R1 nedetekuje (spatne)
    //        //tim padem bude nepravem odebrano containsLicense/contains_licenses predkum R1, prestoze by na to meli narok kvuli Ix
    //        String ancestorPidEscaped = ancestorPid.replace(":", "\\:");
    //        String targetPidEscaped = targetPid.replace(":", "\\:");
    //        String q = String.format(
    //                "licenses:%s AND (pid_paths:%s/* OR pid_paths:*/%s/*) AND -pid:%s AND -pid_paths:%s/* AND -pid_paths:*/%s/*",
    //               license, ancestorPidEscaped, ancestorPidEscaped, targetPidEscaped, targetPidEscaped, targetPidEscaped);
    //        String query = "fl=pid&rows=0&q=" + URLEncoder.encode(q, "UTF-8");
    //        JSONObject jsonObject = searchIndex.requestWithSelectReturningJson(query);
    //        JSONObject response = jsonObject.getJSONObject("response");
    //        if (response.getInt("numFound") == 0) {
    //            ancestorsWithoutLicenceFromAnotherDescendant.add(ancestorPid);
    //        }
    //    }
    //    return ancestorsWithoutLicenceFromAnotherDescendant;
    //}


}
