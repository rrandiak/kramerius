package cz.inovatika.sdnnt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import cz.incad.kramerius.utils.conf.KConfiguration;
import cz.inovatika.sdnnt.utils.SDNNTCheckUtils;
import cz.kramerius.searchIndex.indexer.SolrConfig;

public class SDNNTFetch {
    
    //public static final SyncConfig KNAV = new SyncConfig("https://kramerius.lib.cas.cz/search/" , "v5", "knav");
    
    private static final SimpleDateFormat S_DATE_FORMAT = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

    public static final Logger LOGGER = Logger.getLogger(SDNNTFetch.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException, SolrServerException {
        System.setProperty("solr.cloud.client.stallTime", "119999");

        String sdnntHost  = KConfiguration.getInstance().getConfiguration().getString("solrSdnntHost");
        if (sdnntHost == null) {
            throw new IllegalStateException("Missing configuration key 'solrSdnntHost'");
        }
        
        String[] splitted = sdnntHost.split("/");
        String collection = splitted.length > 0 ? splitted[splitted.length -1] : null;
        if (collection != null) {
            int index = sdnntHost.indexOf(collection);
            if (index > -1) { sdnntHost = sdnntHost.substring(0, index); }
        }
        HttpSolrClient client = new HttpSolrClient.Builder(sdnntHost).build();
        try {
            process(args, client, new SyncConfig());
        } finally {
            client.close();
        }
    }



    public static void process(String[] args, HttpSolrClient client, SyncConfig config) throws IOException, InterruptedException, SolrServerException {

        if (args.length >= 1) {
            String sdnntEndpoint = args[0];
            long start = System.currentTimeMillis();
            
            //Map<String,List<String>> identifiers = new HashMap<>();
            Map<String,String> pids2idents = new HashMap<>();
            
            LOGGER.info("Connecting sdnnt list and iterating serials ");
            iterateSDNNTFormat(client, config, sdnntEndpoint,  "SE", start, pids2idents);
            LOGGER.info("Connecting sdnnt list and iterating books ");
            iterateSDNNTFormat(client, config, sdnntEndpoint,  "BK", start, pids2idents);

            long stop = System.currentTimeMillis();
            LOGGER.info("List fetched. It took " + (stop - start) + " ms");
            
            OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(new Date(start).toInstant(), ZoneId.systemDefault());
            String format = DateTimeFormatter.ISO_INSTANT.format(offsetDateTime);
            client.deleteByQuery(config.getSyncCollection(), String.format("fetched:[* TO %s-1MINUTE] AND type:(main OR granularity)", format));
            
            if (config.getSyncCollection() != null) client.commit(config.getSyncCollection());

            if (config.getBaseUrl() != null) {
                LicenseAPIFetcher apiFetcher = LicenseAPIFetcher.Versions.valueOf(config.getVersion()).build(config.getBaseUrl(), config.getVersion());
                Map<String, List<String>> licenses = apiFetcher.check(pids2idents.keySet());
                List<String> fetchedPids = new ArrayList<>(licenses.keySet());
                
                int setsize = licenses.size();
                int batchsize = 100;
                int numberofiteration = setsize / batchsize;
                if (setsize % batchsize != 0) numberofiteration = numberofiteration + 1;
                for (int i = 0; i < numberofiteration; i++) {
                  int from = i*batchsize;
                  int to = Math.min((i+1)*batchsize, setsize);
                  List<String> subList = fetchedPids.subList(from, to);

                  List<String> changedIdents = new ArrayList<>();
                  Map<String, SolrInputDocument> allChanges = new HashMap<>();
                  for (int j = 0; j < subList.size(); j++) {
                      String pid = subList.get(j);
                      String ident = pids2idents.get(pid);
                      if (ident != null) {
                          
                          SolrInputDocument idoc = new SolrInputDocument();
                          idoc.setField("id", ident);
                          
                          List<String> pidLicenses = licenses.get(pid);
                          if (pidLicenses != null && !pidLicenses.isEmpty()) {
                              
                              LOGGER.info("Updating document "+ident);
                              pidLicenses.stream().forEach(lic-> {
                                  atomicAddDistinct(idoc, lic, "real_kram_licenses");
                              });
                              
                          }
                          atomicSet(idoc, true, "real_kram_exists");
                          changedIdents.add(ident);
                          allChanges.put(ident, idoc);
                      }
                  }
                  

                  SolrDocumentList list = client.getById(config.getSyncCollection(), changedIdents);
                  for (SolrDocument rDoc : list) {
                    Object ident = rDoc.getFieldValue("id");
                    SolrInputDocument in = allChanges.get(ident.toString());

                    Collection<Object> fieldValues = in.getFieldValues("real_kram_licenses");
                    List<String> docLicenses = fieldValues != null ? fieldValues.stream()
                            .map(obj-> {
                                Map<String,String> m = (Map<String, String>) obj;
                                return m.get("add-distinct");
                            }).collect(Collectors.toList()) : new ArrayList<>();

                    Object type = rDoc.getFieldValue("type");
                    boolean granularityChange = type != null ? type.toString().equals("granularity") : false;
                    boolean dirty = false;
                    Object rDocState = rDoc.getFieldValue("state");
                    if (rDocState!= null &&  rDocState.toString().equals("A")) {
                        Object license = rDoc.getFieldValue("license");
                        if (license != null) {
                            if (license.toString().equals("dnntt") && !docLicenses.contains("dnntt")) {
                                if (docLicenses.contains("dnnto")) {
                                    atomicAddDistinct(in, "change_dnnto_dnntt", "sync_actions");
                                    dirty = true;
                                } else {
                                    atomicAddDistinct(in, "add_dnntt", "sync_actions");
                                    dirty = true;
                                }
                            }

                            if (license.toString().equals("dnnto") && !docLicenses.contains("dnnto")) {
                                if (docLicenses.contains("dnntt")) {
                                    atomicAddDistinct(in, "change_dnnto_dnntt", "sync_actions");
                                    dirty = true;
                                } else {
                                    atomicAddDistinct(in, "add_dnnto", "sync_actions");
                                    dirty = true;
                                }
                            }
                        }
                    } else {
                       // neocekavam licencece 
                        if (docLicenses.contains("dnntt")) {
                            atomicAddDistinct(in, "remove_dnntt", "sync_actions");
                            dirty = true;
                        }
                        if (docLicenses.contains("dnnto")) {
                            atomicAddDistinct(in, "remove_dnnto", "sync_actions");
                            dirty = true;
                        }
                    }
                    
                    if (dirty && granularityChange) {
                        Object field = rDoc.getFieldValue("parent_id"); 
                        if (field!= null) {
                            if (changedIdents.contains(field.toString())) {
                                SolrInputDocument masterIn = allChanges.get(field.toString());
                                // pozmenime
                                Collection<Object> masterInSyncActions = masterIn.getFieldValues("sync_actions");
                                List<String> actions = masterInSyncActions != null ? masterInSyncActions.stream()
                                        .map(obj-> {
                                            Map<String,String> m = (Map<String, String>) obj;
                                            return m.get("add-distinct");
                                        }).collect(Collectors.toList()) : new ArrayList<>();
                                if (!actions.contains("partial_change")) {
                                    atomicAddDistinct(masterIn, "partial_change", "sync_actions");
                                }
                                //if (fieldValue.con)
                            } else {
                                SolrInputDocument masterIn = new SolrInputDocument();
                                masterIn.setField("id", field.toString());
                                atomicAddDistinct(masterIn, "partial_change", "sync_actions");
                                changedIdents.add(field.toString());
                                allChanges.put(field.toString(), masterIn);
                            }
                        }
                    }
                  }
                  
                  
                  if (!changedIdents.isEmpty()) {
                      UpdateRequest req = new UpdateRequest();
                      changedIdents.forEach(ident-> {
                          req.add(allChanges.get(ident));
                      });
                      
                      try {
                          UpdateResponse response = req.process(client, config.getSyncCollection());
                          LOGGER.info("qtime:"+response.getQTime());
                          if (config.getSyncCollection() != null) client.commit(config.getSyncCollection());
                      } catch (SolrServerException  | IOException e) {
                          LOGGER.log(Level.SEVERE,e.getMessage());
                      }
                  }
              }
              
            }
        } else {
            LOGGER.warning("Expecting two parameters. <sdnnt_endpoint>, <solr_endpoint> <folder>");
        }
    }

    public static void atomicSet(SolrInputDocument idoc, Object fValue, String fName) {
        Map<String, Object> modifier = new HashMap<>(1);
        modifier.put("set", fValue);
        idoc.addField(fName, modifier);
    }

    public static void atomicAddDistinct(SolrInputDocument idoc, Object fValue, String fName) {
        Map<String, Object> modifier = new HashMap<>(1);
        modifier.put("add-distinct", fValue);
        idoc.addField(fName, modifier);
    }


    
    public static File throttle(Client client,  String url) throws IOException, InterruptedException {

        int max_repetion = 3;
        int seconds = 5;

        for (int i = 0; i < max_repetion; i++) {
            WebResource r = client.resource(url);
            ClientResponse response = r.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
            int status = response.getStatus();
            if (status == Status.OK.getStatusCode()) {
                String entity = response.getEntity(String.class);
                File tmpFile = File.createTempFile("sdnnt", "resp");
                tmpFile.deleteOnExit();
                IOUtils.write(entity.getBytes(Charset.forName("UTF-8")), new FileOutputStream(tmpFile));
                return tmpFile;
            } else if (status == 409) {
                // wait
                int sleep = KConfiguration.getInstance().getConfiguration().getInt("sdnnt.throttle.wait", 720000);
                LOGGER.info("Server is too busy; waiting for "+(sleep/1000/60)+" min");
                Thread.sleep(sleep);
            }
        }
        throw new IllegalStateException("Maximum number of waiting exceeed");
    }
    
    
    
    private static void iterateSDNNTFormat(
            HttpSolrClient client,
            SyncConfig config, 
            String sdnntChangesEndpoint, 
            String format, 
            long startProcess,
            Map<String,String> pids2oais) throws IOException, InterruptedException, SolrServerException {
        List<SolrInputDocument> docs = new ArrayList<>();
        Map<String, AtomicInteger> counters = new HashMap<>();
        
        int sum = 0;
        String token = "*";
        String prevToken = "";
        Client c = Client.create();
        LOGGER.info(String.format("SDNNT changes endpoint is %s", sdnntChangesEndpoint));
        String sdnntApiEndpoint = sdnntChangesEndpoint + "?format=" + format + "&rows=1000&resumptionToken=%s&digital_library="+config.getAcronym();
        while (token != null && !token.equals(prevToken)) {
            String formatted = String.format(sdnntApiEndpoint, token);
            File file = throttle(c, formatted);
            String response = FileUtils.readFileToString(file, Charset.forName("UTF-8"));
            JSONObject resObject = new JSONObject(response);

            prevToken = token;
            token = resObject.optString("resumptiontoken");
            
            JSONArray items = resObject.getJSONArray("items");
            sum = sum+items.length();
            System.out.println("Size :"+items.length() +" and sum:"+(sum));
            for (int i = 0; i < items.length(); i++) {
                
                //List<String> apids = new ArrayList<>();
                
                JSONObject mainObject = items.getJSONObject(i);
                String ident = mainObject.getString("catalog_identifier");
                if (!counters.containsKey(ident)) {
                    counters.put(ident, new AtomicInteger(0));
                }
                counters.get(ident).addAndGet(1);
                
                SolrInputDocument doc = new SolrInputDocument();
                doc.setField("id", ident+"_"+counters.get(ident).get());
                
                doc.setField("catalog", mainObject.getString("catalog_identifier"));
                doc.setField("title", mainObject.getString("title"));
                doc.setField("type_of_rec", mainObject.getString("type"));
                doc.setField("state", mainObject.getString("state"));
                doc.setField("state", mainObject.getString("state"));
                doc.setField("fetched", new Date(startProcess));
                
                
                
                if (mainObject.has("pid")) {
                    doc.setField("pid", mainObject.getString("pid"));
                    pids2oais.put(mainObject.getString("pid"), ident+"_"+counters.get(ident).get());
                }
                
                if (mainObject.has("license")) {
                    doc.setField("license", mainObject.getString("license"));
                }
                doc.setField("type", "main");
                docs.add(doc);
                
                if (mainObject.has("granularity")) {
                    
                    JSONArray gr = mainObject.getJSONArray("granularity");
                    for (int j = 0; j < gr.length(); j++) {
                        JSONObject item = gr.getJSONObject(j);
                        SolrInputDocument gDod = new SolrInputDocument();
                        gDod.setField("parent_id", ident+"_"+counters.get(ident).get());
                        if (item.has("states")) {
                            Object state = item.get("states");
                            if (state instanceof JSONArray) {
                                JSONArray stateArr = (JSONArray) state;
                                if (stateArr.length() > 0) {
                                    gDod.setField("state", stateArr.getString(0));
                                }
                            } else {
                                gDod.setField("state", state.toString());
                            }
                        }
                        
                        gDod.setField("type", "granularity");
                        
                        if (item.has("pid")) {
                            gDod.setField("pid", item.getString("pid"));
                            pids2oais.put(item.getString("pid"), ident+"_"+counters.get(ident).get()+"_"+item.getString("pid"));
                            gDod.setField("id", ident+"_"+counters.get(ident).get()+"_"+item.getString("pid"));
                        }
                        
                        if (item.has("license")) {
                            gDod.setField("license", item.getString("license"));
                        }
                        gDod.setField("fetched", new Date(startProcess));
                        
                        docs.add(gDod);
                    }
                }
            }
        }
        
        if (docs.size() > 0) {
            int setsize = docs.size();
            int batchsize = 10000;
            int numberofiteration = docs.size() / batchsize;
            if (setsize % batchsize != 0) numberofiteration = numberofiteration + 1;
            for (int i = 0; i < numberofiteration; i++) {
                int from = i*batchsize;
                int to = Math.min((i+1)*batchsize, setsize);
                List<SolrInputDocument> batchDocs = docs.subList(from, to);
                LOGGER.info(String.format("Updating records %d - %d and size %d", from, to,batchDocs.size()));
                
                UpdateRequest req = new UpdateRequest();
                for (SolrInputDocument bDoc : batchDocs) {
                    req.add(bDoc);
                }
                try {
                    UpdateResponse response = req.process(client, config.getSyncCollection());
                    LOGGER.info("qtime:"+response.getQTime());
                } catch (SolrServerException  | IOException e) {
                    LOGGER.log(Level.SEVERE,e.getMessage());
                }
            }
        }
    }

    private static String gItemState(JSONObject gItem) {
        if (gItem.has("states")) {
            Object object = gItem.get("states");
            if (object instanceof JSONArray) {
                JSONArray jsArray = (JSONArray) object;
                return jsArray.getString(0);
            } else {
                return object.toString();
            }
        }
        return null;
    }
}
