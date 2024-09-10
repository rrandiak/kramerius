/*
 * Copyright (C) 2013 Pavel Stastny
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.incad.kramerius.rest.apiNew.admin.v70.statistics;

import static org.apache.http.HttpStatus.SC_BAD_REQUEST;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.text.ParseException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import com.google.inject.name.Named;
import cz.incad.kramerius.processes.LRProcessManager;
import cz.incad.kramerius.processes.*;

import cz.incad.kramerius.rest.api.exceptions.BadRequestException;
import cz.incad.kramerius.rest.api.processes.LRResource;
import cz.incad.kramerius.rest.apiNew.client.v70.SearchResource;
import cz.incad.kramerius.rest.apiNew.exceptions.InternalErrorException;
import cz.incad.kramerius.statistics.filters.*;
import cz.incad.kramerius.statistics.formatters.report.StatisticsReportFormatter;
import cz.incad.kramerius.users.LoggedUsersSingleton;
import cz.incad.kramerius.utils.IOUtils;
import cz.incad.kramerius.utils.conf.KConfiguration;

import org.apache.http.client.HttpResponseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.terracotta.statistics.Statistic;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.google.inject.Inject;
import com.google.inject.Provider;

import cz.incad.kramerius.ObjectPidsPath;
import cz.incad.kramerius.SolrAccess;
import cz.incad.kramerius.gdpr.AnonymizationSupport;
import cz.incad.kramerius.rest.api.exceptions.ActionNotAllowed;
import cz.incad.kramerius.rest.api.exceptions.GenericApplicationException;
import cz.incad.kramerius.security.RightsResolver;
import cz.incad.kramerius.security.SecuredActions;
import cz.incad.kramerius.security.SpecialObjects;
import cz.incad.kramerius.security.User;
import cz.incad.kramerius.statistics.ReportedAction;
import cz.incad.kramerius.statistics.StatisticReport;
import cz.incad.kramerius.statistics.StatisticsAccessLog;
import cz.incad.kramerius.statistics.StatisticsReportException;
import cz.incad.kramerius.statistics.filters.VisibilityFilter.VisbilityType;
import cz.incad.kramerius.utils.StringUtils;
import cz.incad.kramerius.utils.XMLUtils;
import cz.incad.kramerius.utils.database.Offset;


/**
 * Moved from k5
 * @author happy
 *
 */
@Path("/admin/v7.0/statistics")
public class StatisticsResource {

    public static final Logger LOGGER = Logger.getLogger(StatisticsResource.class.getName());
    
    public static final Semaphore STATISTIC_SEMAPHORE = new Semaphore(1);


    @Inject
    @Named("database")
    StatisticsAccessLog statisticsAccessLog;

    @Inject
    RightsResolver rightsResolver;

    @Inject
    Provider<User> userProvider;

    @Inject
    Set<StatisticsReportFormatter> reportFormatters;

    @Inject
    LRProcessManager lrProcessManager;

//    @Inject
//    LoggedUsersSingleton loggedUsersSingleton;

    @Inject
    Provider<HttpServletRequest> requestProvider;

    @Inject
    @Named("new-index")
    private SolrAccess solrAccess;

    
    @DELETE
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response cleanData(@QueryParam("dateFrom") String dateFrom, @QueryParam("dateTo") String dateTo) {
        // mel by byt nekdo jiny nez ten kdo resi prohlizeni statistik
        if (permit(SecuredActions.A_STATISTICS_EDIT)) {
            try {
                // kontrola datumu / break /pokud je vetsi nez break, chybny dotaz
                if (StringUtils.isAnyString(dateFrom) && StringUtils.isAnyString(dateTo)) {
                    try {
                        Date dateFromd = StatisticReport.DATE_FORMAT.parse(dateFrom);
                        Date dateTod = StatisticReport.DATE_FORMAT.parse(dateTo);
                        int deletedResult = this.statisticsAccessLog.cleanData(dateFromd, dateTod);
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("deleted", deletedResult);
                        return Response.ok().entity(jsonObject.toString()).build();
                    } catch (ParseException e) {
                        throw new BadRequestException("parsing exception dateFrom, dateTo");
                    }
                } else {
                    throw new BadRequestException("expecting parameters dateFrom, dateTo");
                }
            } catch (IOException e) {
                throw new GenericApplicationException(e.getMessage());
            }
        } else {
            throw new ActionNotAllowed("not allowed");
        }
    }

    
    
    @GET
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response reports() {
        if (permit(SecuredActions.A_STATISTICS)) {
            StatisticReport[] allReports = this.statisticsAccessLog.getAllReports();
            List<String> ids = Arrays.asList(allReports).stream().map(StatisticReport::getReportId)
                    .collect(Collectors.toList());
            JSONObject jsonObject = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            ids.stream().forEach(jsonArray::put);
            jsonObject.put("reports", jsonArray);
            return Response.ok().entity(jsonObject.toString()).build();
        } else {
            throw new ActionNotAllowed("not allowed");
        }
    }
    
    @GET
    @Path("{report}/options")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response filters(
            @PathParam("report") String rip,
            @QueryParam("action") String raction,
            @QueryParam("dateFrom") String dateFrom,
            @QueryParam("dateTo") String dateTo,
            @QueryParam("model") String model,
            @QueryParam("models") String models,
            @QueryParam("identifier") String identifier,
            @QueryParam("license") String license,
            @DefaultValue("ALL") @QueryParam("visibility") String visibility,
            @QueryParam("offset") String filterOffset,
            @QueryParam("pids") String pids) {
        if (permit(SecuredActions.A_STATISTICS)) {
                try {
                    StatisticsFiltersContainer container = container(dateFrom, dateTo, model, visibility,
                            pids, license, models, identifier);
                    StatisticReport report = statisticsAccessLog.getReportById(rip);
                    if (report != null) {
                        List<String> results = report.verifyFilters(raction != null ? ReportedAction.valueOf(raction) : null, container);
                        if (results.isEmpty()) {
                            List<String> optionalValues = report.getOptionalValues(container);
                            JSONArray jsonArray = new JSONArray();
                            optionalValues.stream().forEach(jsonArray::put);
                            JSONObject object = new JSONObject();
                            object.put(report.getReportId(), jsonArray);
                            return Response.ok().entity(object).build();
                        } else {
                            String body = results.stream().collect(Collectors.joining("\n"));
                            return Response.status(Response.Status.BAD_REQUEST).entity(body).build();
                        }
                    } else {
                        return Response.status(Response.Status.NOT_FOUND).build();
                    }
                } catch (JSONException e) {
                    throw new GenericApplicationException(e.getMessage());
                }
            } else {
                throw new ActionNotAllowed("not allowed");
            }
    }

    @GET
    @Path("{report}")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response getReportPage(
            @PathParam("report") String rip,
            @QueryParam("action") String raction,
            @QueryParam("dateFrom") String dateFrom,
            @QueryParam("dateTo") String dateTo,
            @QueryParam("model") String model,
            @QueryParam("models") String models,
            @QueryParam("identifier") String identifier,
            @QueryParam("license") String license,
            @DefaultValue("ALL") @QueryParam("visibility") String visibility,
            @QueryParam("offset") String filterOffset,
            @QueryParam("pids") String pids,
            @QueryParam("resultSize") String filterResultSize) {
        
        if (permit(SecuredActions.A_STATISTICS)) {
            try {
                StatisticsFiltersContainer container = container(dateFrom, dateTo, model, visibility,
                        pids, license, models, identifier);
                StatisticReport report = statisticsAccessLog.getReportById(rip);
                if (report != null) {
                    Offset offset = new Offset("0", "10");
                    if (StringUtils.isAnyString(filterOffset)
                            && StringUtils.isAnyString(filterResultSize)) {
                        offset = new Offset(filterOffset, filterResultSize);
                    }
                    
                    List<String> results = report.verifyFilters(raction != null ? ReportedAction.valueOf(raction) : null, container);
                    if (results.isEmpty()) {
                        List<Map<String, Object>> repPage = report.getReportPage(
                                raction != null ? ReportedAction.valueOf(raction)
                                        : null,container, offset);
                        if (report.convertToObject()) {
                            if (repPage.size() > 1) {
                                JSONArray jsonArr = new JSONArray();
                                for (Map<String, Object> map : repPage) {
                                    JSONObject json = new JSONObject(map);
                                    jsonArr.put(json);
                                }
                                return Response.ok().entity(jsonArr.toString()).build();
                            } else if (repPage.size() == 1){
                                JSONObject json = new JSONObject(repPage.get(0));
                                return Response.ok().entity(json.toString()).build();
                            } else {
                                return Response.ok().entity(new JSONArray().toString()).build();
                            }
                        } else {
                            JSONArray jsonArr = new JSONArray();
                            for (Map<String, Object> map : repPage) {
                                JSONObject json = new JSONObject(map);
                                jsonArr.put(json);
                            }
                            return Response.ok().entity(jsonArr.toString()).build();
                        }
                    } else {
                    	String body = results.stream().collect(Collectors.joining("\n"));
                        return Response.status(Response.Status.BAD_REQUEST).entity(body).build();
                    }
                } else {
                    return Response.status(Response.Status.NOT_FOUND).build();
                }
            } catch (StatisticsReportException e) {
                throw new GenericApplicationException(e.getMessage());
            } catch (JSONException e) {
                throw new GenericApplicationException(e.getMessage());
            }
        } else {
            throw new ActionNotAllowed("not allowed");
        }
    }



    private StatisticsFiltersContainer container(String dateFrom, String dateTo, String model, String visibility,
            String pids, String license, String models, String identifier) {
        DateFilter dateFilter = new DateFilter();
        dateFilter.setFromDate(dateFrom);
        dateFilter.setToDate(dateTo);

        ModelFilter modelFilter = new ModelFilter();
        modelFilter.setModel(model);

        VisibilityFilter visFilter = new VisibilityFilter();
        visFilter.setSelected(VisbilityType.valueOf(visibility));

        PidsFilter pidsFilter = new PidsFilter();
        pidsFilter.setPids(pids);
        LicenseFilter licenseFilter = new LicenseFilter(license);

        MultimodelFilter multiModel = new MultimodelFilter();
        if (models != null) {
            multiModel.setModels(Arrays.asList(models.split(",")));
        }
        
        IdentifiersFilter identFilter= new IdentifiersFilter();
        identFilter.setIdentifier(identifier);
        
        StatisticsFiltersContainer container = new StatisticsFiltersContainer(new StatisticsFilter[] { dateFilter,
                modelFilter, /* ipAddr,uniqueIPFilter, */ visFilter, licenseFilter, multiModel, identFilter });

        return container;
    }


    @GET
    @Path("{report}/export")
    @Consumes({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response exportJSON(@PathParam("report") String rip,
                                  @QueryParam("action") String action,
                                  @QueryParam("dateFrom") String dateFrom,
                                  @QueryParam("dateTo") String dateTo,
                                  @QueryParam("model") String model,
                                  @QueryParam("identifier") String identifier,

                                  @DefaultValue("ALL") @QueryParam("visibility") String visibilityValue,
                                  @QueryParam("annualyear") String annual,
                                  @QueryParam("pids") String pids,
                                  @QueryParam("license") String license,
                                  @DefaultValue("export.data") @QueryParam("file") String file) {

        return export(rip, action, dateFrom, dateTo, model, visibilityValue,  annual,
				pids, file, license, identifier,  MediaType.APPLICATION_JSON);
    }

    @GET
    @Path("{report}/export/json")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response exportJSONPath(@PathParam("report") String rip,
                                  @QueryParam("action") String action,
                                  @QueryParam("dateFrom") String dateFrom,
                                  @QueryParam("dateTo") String dateTo,
                                  @QueryParam("model") String model,
                                  @QueryParam("identifier") String identifier,

                                  @DefaultValue("ALL") @QueryParam("visibility") String visibilityValue,
                                  @QueryParam("annualyear") String annual,
                                  @QueryParam("pids") String pids,
                                  @QueryParam("license") String license,
                                  @DefaultValue("export.data") @QueryParam("file") String file) {

        return export(rip, action, dateFrom, dateTo, model, visibilityValue,  annual,
				pids, file, license, identifier,  MediaType.APPLICATION_JSON);
    }

    @GET
    @Path("{report}/export")
    @Consumes({ "text/csv" })
    @Produces({ "text/csv" })
    public Response exportCSV(@PathParam("report") String rip,
                                  @QueryParam("action") String action,
                                  @QueryParam("dateFrom") String dateFrom,
                                  @QueryParam("dateTo") String dateTo,
                                  @QueryParam("model") String model,
                                  @QueryParam("identifier") String identifier,

                                  @DefaultValue("ALL") @QueryParam("visibility") String visibilityValue,
                                  @QueryParam("annualyear") String annual,
                                  @QueryParam("pids") String pids,
                                  @QueryParam("license") String license,
                                  @DefaultValue("export.data") @QueryParam("file") String file) {

        return export(rip, action, dateFrom, dateTo, model, visibilityValue, annual,pids, file, license,
                identifier, "text/csv");
    }

    @GET
    @Path("{report}/export/csv")
    @Produces({ "text/csv" })
    public Response exportCSVPath(@PathParam("report") String rip,
                                  @QueryParam("action") String action,
                                  @QueryParam("dateFrom") String dateFrom,
                                  @QueryParam("dateTo") String dateTo,
                                  @QueryParam("model") String model,
                                  @QueryParam("identifier") String identifier,

                                  @DefaultValue("ALL") @QueryParam("visibility") String visibilityValue,
                                  @QueryParam("annualyear") String annual,
                                  @QueryParam("license") String license,
                                  @QueryParam("pids") String pids,
                                 
                                  @DefaultValue("export.data") @QueryParam("file") String file) {

        return export(rip, action, dateFrom, dateTo, model, visibilityValue, annual,
				pids, file, license, identifier, "text/csv");
    }

    //public Response delete
    
    
    @GET
    @Path("{report}/export")
    @Consumes({ MediaType.APPLICATION_XML + ";charset=utf-8"})
    @Produces({ MediaType.APPLICATION_XML + ";charset=utf-8" })
    public Response exportXML(@PathParam("report") String rip,
                                  @QueryParam("action") String action,
                                  @QueryParam("dateFrom") String dateFrom,
                                  @QueryParam("dateTo") String dateTo,
                                  @QueryParam("model") String model,
                                  @QueryParam("identifier") String identifier,

                                  @DefaultValue("ALL")  @QueryParam("visibility") String visibilityValue,
                                  @QueryParam("annualyear") String annual,
                                  @QueryParam("pids") String pids,
                                  @QueryParam("license") String license,
                                  @DefaultValue("export.data") @QueryParam("file") String file) {

        return export(rip, action, dateFrom, dateTo, model, visibilityValue,  annual,
				pids, file,license, identifier, MediaType.APPLICATION_XML);
    }


	private Response export(String rip, String action, String dateFrom, String dateTo, String model,
			String visibilityValue,  String annual, String pids,
			String file, String license, String identifier, String format ) {
		AnnualYearFilter annualYearFilter = new AnnualYearFilter();
        annualYearFilter.setAnnualYear(annual);

        DateFilter dateFilter = new DateFilter();
        dateFilter.setFromDate(dateFrom != null && (!dateFrom.trim().equals("")) ? dateFrom : null);
        dateFilter.setToDate(dateTo != null && (!dateTo.trim().equals("")) ? dateTo : null);

        ModelFilter modelFilter = new ModelFilter();
        modelFilter.setModel(model);

        PidsFilter pidsFilter = new PidsFilter();
        pidsFilter.setPids(pids);
        
        IdentifiersFilter idFilter = new IdentifiersFilter();
        idFilter.setIdentifier(identifier);


        if (action != null && action.equals("null")) {
            action = null;
        }

        if (dateFrom == null) {
            dateFrom = "";
        }

        if (dateTo == null) {
            dateTo = "";
        }

        MultimodelFilter multimodelFilter = new MultimodelFilter();

		LicenseFilter licenseFilter = new LicenseFilter(license);

        VisibilityFilter visFilter = new VisibilityFilter();
		visFilter.setSelected(VisbilityType.valueOf(visibilityValue));

        if (permit(SecuredActions.A_STATISTICS)) {
            if (rip != null && (!rip.equals(""))) {
            	StatisticReport report = this.statisticsAccessLog.getReportById(rip);
            	Optional<StatisticsReportFormatter> opts = reportFormatters.stream().filter(formatter -> {
                    return formatter.getMimeType().contains(format) && formatter.getReportId().equals(report.getReportId());
                }).findAny();
            	
                if (opts.isPresent()) {

                    List<String> results = report.verifyFilters(null, new StatisticsFiltersContainer(new StatisticsFilter []{dateFilter,modelFilter,visFilter, multimodelFilter, annualYearFilter,  pidsFilter, idFilter}));
                    if (results.isEmpty()) {
                    	StatisticsReportFormatter selectedFormatter =  opts.get();
                        String info = "";

                        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

                        try {
                            STATISTIC_SEMAPHORE.acquire();
                            selectedFormatter.addInfo(bos, info);
                            selectedFormatter.beforeProcess(bos);

                            // Must be synchronized - only one report at the time
                            //report.prepareViews(action != null ? ReportedAction.valueOf(action) : null,new StatisticsFiltersContainer(new StatisticsFilter []{dateFilter,modelFilter,  multimodelFilter, annualYearFilter, pidsFilter}));
                            report.processAccessLog(action != null ? ReportedAction.valueOf(action) : null, selectedFormatter,new StatisticsFiltersContainer(new StatisticsFilter []{dateFilter,modelFilter,visFilter, multimodelFilter, annualYearFilter,  pidsFilter, licenseFilter, idFilter}));
                            selectedFormatter.afterProcess(bos);

                            return Response.ok(new StreamingOutput() {
                                @Override
                                public void write(OutputStream output) throws IOException, WebApplicationException {
                                    IOUtils.copyStreams(new ByteArrayInputStream(bos.toByteArray()), output);
                                }
                            }).header("Content-disposition",  "attachment; filename="+file).build();
                        } catch (IOException e) {
                            throw new GenericApplicationException(e.getMessage());
                        } catch (StatisticsReportException e) {
                            throw new GenericApplicationException(e.getMessage());
                        } catch (InterruptedException e) {
                            throw new GenericApplicationException(e.getMessage());
                        } finally {
                            STATISTIC_SEMAPHORE.release();
                        }
                    } else {
                    	String body = results.stream().collect(Collectors.joining("\n"));
                        return Response.status(Response.Status.BAD_REQUEST).entity(body).build();
                    }
                } else {
                    throw new BadRequestException("For selected report and selected mimtype I cannot find the formatter");
                }
            } else {
                throw new BadRequestException("No report selected");
            }
        } else {
            throw new ActionNotAllowed("not allowed");
       }
	}


	public static void validateDateRange(String dateFrom, String dateTo) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        OffsetDateTime fromDate = OffsetDateTime.parse(dateFrom, formatter);
        OffsetDateTime toDate = OffsetDateTime.parse(dateTo, formatter);
        long maxHours = 120;
        long hoursBetween = ChronoUnit.HOURS.between(fromDate, toDate);
        if (hoursBetween >= maxHours) {
            throw new BadRequestException( String.format("The difference between dateFrom and dateTo must be less than %s hours.", maxHours));
        }
    }
	
	
	
	
    @GET
    @Path("search")
    public Response search(@Context UriInfo uriInfo, @Context HttpHeaders headers, @QueryParam("wt") String wt) {
        try {
            if (permit(SecuredActions.A_STATISTICS)) {
                MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
                StringBuilder builder = new StringBuilder();
                Set<String> keys = queryParameters.keySet();
                for (String k : keys) {
                    for (final String v : queryParameters.get(k)) {
                        String value = v;
                        builder.append(k).append("=").append(URLEncoder.encode(value, "UTF-8"));
                        builder.append("&");
                    }
                }
                
                if ("json".equals(wt)) {
                    return Response.ok().type(MediaType.APPLICATION_JSON + ";charset=utf-8").entity(buildSearchResponseJson(uriInfo, builder.toString() )).build();
                } else if ("xml".equals(wt)) {
                    return Response.ok().type(MediaType.APPLICATION_XML + ";charset=utf-8").entity(buildSearchResponseXml(uriInfo, builder.toString())).build();
                } else { //format not specified in query param "wt"
                    boolean preferXmlAccordingToHeaderAccept = false;
                    List<String> headerAcceptValues = headers.getRequestHeader("Accept");
                    if (headerAcceptValues != null) { //can be null instead of empty list in some implementations
                        for (String headerValue : headerAcceptValues) {
                            if ("application/xml".equals(headerValue) || "text/xml".equals(headerValue)) {
                                preferXmlAccordingToHeaderAccept = true;
                                break;
                            }
                        }
                    }
                    if (preferXmlAccordingToHeaderAccept) { //header Accept contains "application/xml" or "text/xml"
                        return Response.ok().type(MediaType.APPLICATION_XML + ";charset=utf-8").entity(buildSearchResponseXml(uriInfo, builder.toString())).build();
                    } else { //default format: json
                        return Response.ok().type(MediaType.APPLICATION_JSON + ";charset=utf-8").entity(buildSearchResponseJson(uriInfo, builder.toString())).build();
                    }
                }
            } else {
                throw new ActionNotAllowed("not allowed");
            }
            
        } catch (WebApplicationException e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalErrorException(e.getMessage());
        }
    }
    
    private String buildSearchResponseJson(UriInfo uriInfo, String solrQuery) {
        try {
            return cz.incad.kramerius.utils.solr.SolrUtils.requestWithSelectReturningString(logsEndpoint(), solrQuery, "json");
            
        } catch (HttpResponseException e) {
            if (e.getStatusCode() == SC_BAD_REQUEST) {
                LOGGER.log(Level.INFO, "SOLR Bad Request: " + uriInfo.getRequestUri());
                throw new BadRequestException(e.getMessage());
            } else {
                LOGGER.log(Level.INFO, e.getMessage(), e);
                throw new InternalErrorException(e.getMessage());
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalErrorException(e.getMessage());
        } catch (JSONException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalErrorException(e.getMessage());
        }
    }


    private String buildSearchResponseXml(UriInfo uriInfo,String solrQuery) {
        try {
            return cz.incad.kramerius.utils.solr.SolrUtils.requestWithSelectReturningString(logsEndpoint(), solrQuery, "xml");
        } catch (HttpResponseException e) {
            if (e.getStatusCode() == SC_BAD_REQUEST) {
                LOGGER.log(Level.INFO, "SOLR Bad Request: " + uriInfo.getRequestUri());
                throw new BadRequestException(e.getMessage());
            } else {
                LOGGER.log(Level.INFO, e.getMessage(), e);
                throw new InternalErrorException(e.getMessage());
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalErrorException(e.getMessage());
        }
    }

    
	
    @GET
    @Path("logs")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
    public Response exportLogs(@QueryParam("dateFrom") String dateFrom, @QueryParam("dateTo") String dateTo, @QueryParam("rows") String rows, @QueryParam("start") String start) {
        try {
            
            if (permit(SecuredActions.A_EXPORT_STATISTICS)) {
                if (!StringUtils.isAnyString(rows)) { rows = "10"; }
                if (!StringUtils.isAnyString(start)) { start="0"; }
                String selectEndpint = this.logsEndpoint();
                StringBuilder builder = new StringBuilder("q=*");
                builder.append(String.format("&rows=%s&start=%s", rows, start));
                if (StringUtils.isAnyString(dateFrom) && StringUtils.isAnyString(dateTo)) {
                    
                    validateDateRange(dateFrom, dateTo);

                    //TODO: Change key - this k
                    List<Object> anonymization = KConfiguration.getInstance().getConfiguration().getList("nkp.logs.anonymization", AnonymizationSupport.DEFAULT_ANONYMIZATION_PROPERTIES);
                    List<String> keys = anonymization.stream().map(Object::toString).collect(Collectors.toList());
                    
                    
                    String encoded = URLEncoder.encode(String.format("date:[%s TO %s]", dateFrom, dateTo), "UTF-8");
                    builder.append("&fq="+encoded);
                    InputStream iStream = cz.incad.kramerius.utils.solr.SolrUtils.requestWithSelectReturningStream(selectEndpint, builder.toString(), "json");
                    String string = org.apache.commons.io.IOUtils.toString(iStream, "UTF-8");
                    
                    JSONObject allResp = new JSONObject(string);
                    JSONObject responseObj = allResp.getJSONObject("response");
                    JSONArray docsArray = responseObj.getJSONArray("docs");
                    for (int i = 0; i < docsArray.length(); i++) {
                        JSONObject doc = docsArray.getJSONObject(i);
                        String userSessionAttributes = doc.getString("user_session_attributes");
                        JSONObject changedObj = AnonymizationSupport.annonymizeObject(keys, userSessionAttributes);
                        doc.put("user_session_attributes", changedObj.toString());
                    }
                    return Response.ok().entity(string).build();
                } else {
                    throw new BadRequestException("Expecting 'dateFrom' and 'dateTo'");
                }
            } else {
                throw new ActionNotAllowed("not allowed");
            }
        } catch (WebApplicationException e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalErrorException(e.getMessage());
        }
    }
    
    

    protected String logsEndpoint() {
        String loggerPoint = KConfiguration.getInstance().getProperty("k7.log.solr.point","http://localhost:8983/solr/logs");
        String selectEndpoint = loggerPoint + (loggerPoint.endsWith("/") ? "" : "/" ) +"";
        return selectEndpoint;
    }

    boolean permit(SecuredActions action) {
        User user = null;
        String authToken = this.requestProvider.get().getHeader(LRResource.AUTH_TOKEN_HEADER_KEY);
        if (authToken != null && !lrProcessManager.isAuthTokenClosed(authToken)) {
            String sessionKey = lrProcessManager.getSessionKey(authToken);
            if (sessionKey != null) {
                user = this.userProvider.get();
            } else {
                user = this.userProvider.get();
            }
        } else {
            user = this.userProvider.get();
        }
        return permit(user,action);
    }


    boolean permit(User user, SecuredActions action) {
        if (user != null)
            return this.rightsResolver.isActionAllowed(user,
                    action.getFormalName(),
                    SpecialObjects.REPOSITORY.getPid(), null,
                    ObjectPidsPath.REPOSITORY_PATH).flag();
        else
            return false;
    }
    

    
}
