package com.github.deeprot.integration.camel;



import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.api.dto.GlobalObjects;
import org.apache.camel.component.salesforce.api.dto.SObject;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.Version;
import org.apache.camel.component.salesforce.api.dto.Versions;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchStateEnum;
import org.apache.camel.component.salesforce.api.dto.bulk.ContentType;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.OperationEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.github.deeprot.integration.camel.dto.QueryRecords;
import com.github.deeprot.integration.camel.dto.SalesforceCredentials;
import com.github.deeprot.integration.helper.SalesforceObjectHelper;
import com.mongodb.BasicDBObject;

/**
 * Main processor for the Salesforce integration.
 * 
 * @author bruce.nguyen
 *
 */
@Component
public class SalesforceProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(SalesforceProcessor.class);
	private static final List<String> UNSUPPORTED_OBJECTS = Arrays.asList(
			"AcceptedEventRelation", "ActivityHistory", "AggregateResult",
			"AttachedContentDocument", "CaseStatus", "CombinedAttachment",
			"ContractStatus", "DeclinedEventRelation", "EmailStatus",
			"LeadStatus", "LookedUpFromActivity", "Name", "NoteAndAttachment",
			"OpenActivity", "OwnedContentDocument", "PartnerRole",
			"ProcessInstanceHistory", "RecentlyViewed", "SolutionStatus",
			"TaskPriority", "TaskStatus", "UndecidedEventRelation",
			"UserRecordAccess");
	public static final String HEADER_CREDENTIALS = "credentials";
	public static final int CONNECTION_TIMEOUT = 900_000; // 15 minutes
	public static final int RESPONSE_TIMEOUT = 900_000; // 15 minutes
	public enum DatasetType {
		OBJECT, REPORT;
	}
	public enum ReportType {
		TABULAR, SUMMARY, MATRIX, JOINED;
	}
	
	
	/*
	 * Salesforce REST API endpoints
	 */
	public static final String FROM_COMPONENT = "direct:";
	public static final String FROM_URI_FAILED_LOGIN = "failedLogin";
	public static final String FROM_URI_GET_VERSIONS = "getVersions";
	public static final String FROM_URI_GET_GLOBAL_OBJECTS = "getGlobalObjects";
	public static final String FROM_URI_GET_DESCRIPTION = "describeObject";
	public static final String FROM_URI_COUNT_OBJECTS = "countObject";
	public static final String FROM_URI_GET_OBJECT = "getObject";
	public static final String FROM_URI_GET_OBJECT_WINDOW = "getObjectWindow";
	/*
	 * Salesforce Bulk API endpoints
	 */
	public static final String FROM_URI_CREATE_JOB = "bulkCreateJob";
	public static final String FROM_URI_CLOSE_JOB = "bulkCloseJob";
	public static final String FROM_URI_CREATE_BATCH = "bulkCreateBatch";
	public static final String FROM_URI_CHECK_BATCH_STATUS = "bulkGetBatch";
	public static final String FROM_URI_GET_BATCH_RESULTS = "bulkGetBatchResults";
	public static final String FROM_URI_GET_BATCH_DATA = "bulkGetBatchData";
	/*
	 * Salesforce Report API
	 */
	public static final String FROM_URI_LIST_REPORTS = "listReports";
	public static final String FROM_URI_CREATE_REPORT_INSTANCE = "createReportInstance";
	public static final String FROM_URI_GET_INSTANCE_DATA = "getInstanceData";	
	
	@Autowired
	ProducerTemplate template;

	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_versions.htm"
	 * >Versions</a> via the Camel SF component
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @return the collection of
	 *         {@link org.apache.camel.component.salesforce.api.dto.Version}
	 *         object
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Collection<Version> getVersions(SalesforceCredentials creds) throws Exception {
		List<Version> versions = null;
		try {
			Object o = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_VERSIONS, (Object) null, HEADER_CREDENTIALS, creds);
			if (o instanceof Versions) {
				versions = ((Versions) o).getVersions();
			} else {
				versions = (List<Version>) o;
			}
			debug(versions);
		} catch (CamelExecutionException camelEx) {
			LOG.warn("User failed to log in to Salesforce account using the provided credentials.", camelEx);
			// release the resource
			template.sendBodyAndHeader(FROM_COMPONENT + FROM_URI_FAILED_LOGIN, null, HEADER_CREDENTIALS, creds);
			throw camelEx;
		}
		return versions;
	}

	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_describeGlobal.htm"
	 * >Describe Global</a> via the Camel SF component
	 * <p>
	 * Return the list of SF objects used by the user, or one SF object if
	 * specified.
	 * </p>
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param objectName
	 *            If empty, return the whole SObject lists used by the owner
	 * @return the collection of
	 *         {@link org.apache.camel.component.salesforce.api.dto.SObject}
	 *         object
	 * @throws Exception
	 */
	public Collection<SObject> getObjectTypes(SalesforceCredentials creds, String objectName) { //throws Exception {
		GlobalObjects globalObjects = (GlobalObjects) template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_GLOBAL_OBJECTS, (Object) null, 
				HEADER_CREDENTIALS, creds);
		debug(globalObjects);
		return globalObjects.getSobjects().parallelStream()
				.filter(sObj -> StringUtils.isEmpty(objectName) || objectName.equals(sObj.getName()))
				.filter(sObj -> !UNSUPPORTED_OBJECTS.contains(sObj.getName()))
				.collect(Collectors.toList());
	}

	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_sobject_describe.htm"
	 * >SObject Describe</a> via the Camel SF component
	 * <p>
	 * Return the description of the specified object.
	 * </p>
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param objectName
	 *            the SObject to be described
	 * @return the description object
	 *         {@link org.apache.camel.component.salesforce.api.dto.SObjectDescription}
	 * @throws Exception
	 */
	public SObjectDescription describeObject(SalesforceCredentials creds, final String objectName) throws Exception {
		Assert.notNull(objectName, "objectName should not be null");
		SObjectDescription objectDescription = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_DESCRIPTION, objectName, 
				HEADER_CREDENTIALS, creds, SObjectDescription.class);
		debug(objectDescription);
		return objectDescription;
	}

	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_sobject_basic_info.htm"
	 * >SObject Basic Information</a> via the Camel SF component
	 * <p>
	 * Count the basic information (name and url) of the specified object type.
	 * </p>
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param sObj
	 *            the SObject to be counted
	 * @return the number of instances of the specified SObject
	 * @throws Exception
	 */
	public int countObject(SalesforceCredentials creds, final String objectName) throws Exception {
		Assert.notNull(objectName, "objectName should not be null");
		QueryRecords<?> queryResult = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_COUNT_OBJECTS, objectName, 
				HEADER_CREDENTIALS, creds, QueryRecords.class);
		debug(queryResult);
		return queryResult.getTotalSize();
	}
	
	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_query.htm"
	 * >Query</a> to query the object based on its ID via the Camel SF
	 * component.
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param objectName
	 *            the object name
	 * @param objectId
	 *            the ID of the object
	 * @param fields
	 *            the list of fields to be retrieved
	 * @return the full object data
	 * @throws Exception
	 */
	public Object getObject(SalesforceCredentials creds, final String objectName, final String objectId, final List<String> fields) throws Exception {

		Map<String, Object> headers = new HashMap<String, Object>(3){
			private static final long serialVersionUID = -8085699581668229109L;
			{
				put(HEADER_CREDENTIALS, creds);
				put("objectName", objectName);
				put("fields", StringUtils.collectionToCommaDelimitedString(fields));
			}
		};
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		queryBuilder.append(StringUtils.collectionToCommaDelimitedString(fields))
				.append(" FROM ").append(objectName)
				.append(" WHERE id='").append(objectId).append("'");
		QueryRecords<?> query = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_OBJECT, queryBuilder.toString(), 
				headers, QueryRecords.class);
		debug(query);
		if (query.getTotalSize() > 0) {
			return query.getRecords().get(0);
		} else {
			return null;
		}
	}
	
	/**
	 * Trigger the Salesforce REST API <a href=
	 * "http://www.salesforce.com/us/developer/docs/api_rest/Content/resources_query.htm"
	 * >Query</a> to query a number of objects of a type within a window defined
	 * by limit and offset via the Camel SF component.
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param objectName
	 *            the object name
	 * @param fields
	 *            the list of fields to be retrieved
	 * @param limit
	 *            the window limit
	 * @param offset
	 *            the offset of the window
	 * @return the list of objects and their data
	 * @throws Exception
	 */
	public QueryRecords<?> getObjectWindows(SalesforceCredentials creds, final String objectName, final List<String> fields, int limit, int offset) throws Exception {

		Map<String, Object> headers = new HashMap<String, Object>(3){
			private static final long serialVersionUID = -6937282097678143102L;
			{
				put(HEADER_CREDENTIALS, creds);
				put("objectName", objectName);
				put("fields", StringUtils.collectionToCommaDelimitedString(fields));
			}
		};
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		queryBuilder.append(StringUtils.collectionToCommaDelimitedString(fields))
				.append(" FROM ").append(objectName);
		if (limit > 0 && offset >= 0) {
			queryBuilder.append(" LIMIT ").append(limit);
			queryBuilder.append(" OFFSET ").append(offset);
		}
		QueryRecords<?> query = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_OBJECT_WINDOW, queryBuilder.toString(), 
				headers, QueryRecords.class);
		debug(query);
		return query;
	}
	
	/**
	 * Temporary solutions to fetch the large amount of objects using Salesforce
	 * <a
	 * href="http://www.salesforce.com/us/developer/docs/api_asynch/index.htm"
	 * >Bulk API</a>. The steps involved are: 1) Create a QUERY job 2) Create
	 * batch 3) Check batch status until it is finished 4) Get batch result ID
	 * list 5) Fetch result data
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param objectName
	 *            the object name
	 * @param fields
	 *            the list of fields to be retrieved, limited to <a href=
	 *            "http://www.salesforce.com/us/developer/docs/api_asynch/index.htm"
	 *            >Salesforce boundary</a>
	 * @param limit
	 *            the upper limit
	 * @return the list of objects and their data
	 * @throws Exception
	 */
	public QueryRecords<?> bulkQueryObjects(SalesforceCredentials creds, final String objectName, final List<String> fields, int limit) throws Exception {

		//TODO: should make the whole process asynchronous
		
		QueryRecords<Object> tobeimported = new QueryRecords<Object>();
		tobeimported.setRecords(new ArrayList<Object>());
		
		Map<String, Object> headers = new HashMap<String, Object>(3){
			private static final long serialVersionUID = -6937282097678143102L;
			{
				put(HEADER_CREDENTIALS, creds);
				put("objectName", objectName);
				put("fields", StringUtils.collectionToCommaDelimitedString(fields));
			}
		};
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		queryBuilder.append(StringUtils.collectionToCommaDelimitedString(fields))
				.append(" FROM ").append(objectName);
		if (fields.contains("IsDeleted")) {
			queryBuilder.append(" WHERE IsDeleted = False ");
		}
		if (limit > 0) {
			queryBuilder.append(" LIMIT ").append(limit);
		}
		
		
		/*
		 * Create a QUERY job
		 */
		JobInfo jobInfo = new JobInfo();
        jobInfo.setOperation(OperationEnum.QUERY);
        jobInfo.setContentType(ContentType.XML);
        jobInfo.setObject(objectName);
        jobInfo = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_CREATE_JOB, jobInfo, headers, JobInfo.class);
        LOG.debug("Import job created: " + SalesforceObjectHelper.print(jobInfo));
		/*
		 * Create batch
		 */
        headers.put("jobId", jobInfo.getId());
        headers.put("contentType", ContentType.XML.toString());
        BatchInfo batchInfo = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_CREATE_BATCH, queryBuilder.toString(), headers, BatchInfo.class);
		/*
		 * Check batch status
		 */
        while (batchInfo.getState() == BatchStateEnum.IN_PROGRESS 
        		|| batchInfo.getState() == BatchStateEnum.QUEUED) {
        	// sleep 5 seconds
            Thread.sleep(5000);
        	batchInfo = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_CHECK_BATCH_STATUS, batchInfo, headers, BatchInfo.class);
        }
        LOG.debug("Batch completed : " + SalesforceObjectHelper.print(batchInfo));
        if (batchInfo.getState() == BatchStateEnum.FAILED) {
        	throw new Exception(String.format("Failed to create batch job due to: %s", batchInfo.getStateMessage()));
        }
		/*
		 * Get batch result
		 */
        List<String> resultIds = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_BATCH_RESULTS, batchInfo.getId(), headers, List.class);
		/* 
		 * Fetch and transform data
		 */
        int rowCount = 0;
        Collection<BasicDBObject> jsonObjs = null;
        SObjectDescription oDesc = describeObject(creds, objectName);
        Map<String, String> metadata = oDesc.getFields().stream().collect(Collectors.toMap(f -> f.getName(), f -> f.getType()));
        for (String resultId : resultIds) {
        	headers.put("batchId", batchInfo.getId());
        	long duration = System.currentTimeMillis();
        	InputStream xmlStream = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_BATCH_DATA, resultId, headers, InputStream.class);
        	jsonObjs =  SalesforceObjectHelper.readSalesforceXmlStreamToMongoObject(xmlStream, metadata);
        	duration = System.currentTimeMillis() - duration;
        	LOG.debug("Streaming data for resultset {" + resultId + "} takes : " + (duration/1000) + " seconds");
            rowCount += jsonObjs.size();
            tobeimported.getRecords().addAll(jsonObjs);
        }
        tobeimported.setTotalSize(rowCount);
        /*
         * Close job
         */
        jobInfo = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_CLOSE_JOB, jobInfo, headers, JobInfo.class);
        LOG.debug("Import job completed : " + SalesforceObjectHelper.print(jobInfo));
		return tobeimported;
	}
	
	/**
	 * Trigger the Salesforce Reporting API <a href=
	 * "https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_recentreportslist.htm"
	 * >Report List</a>to get the list of all reports.
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param reportName
	 *            the name of the report to get. If null, get all reports
	 * @return a key-value list of reportId-reportName of all the selected
	 *         reports
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<LinkedHashMap> getReportTypes(SalesforceCredentials creds, String reportName) { //throws Exception {
		StringBuilder query = new StringBuilder("SELECT Id,Name,LastModifiedDate FROM Report WHERE IsDeleted=false"); 
		if (reportName != null && !"".equals(reportName)) {
			query.append(" AND Name LIKE '%").append(reportName).append("%'");
		}
		QueryRecords<?> queryResult = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_LIST_REPORTS, query.toString(), HEADER_CREDENTIALS, creds, QueryRecords.class);
		debug(queryResult);
		return queryResult.getRecords().stream()
				.map(o -> (LinkedHashMap)o).collect(Collectors.toList());
	}
	
	/**
	 * Trigger the Salesforce Reporting API <a href=
	 * "https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_getbasic_reportmetadata.htm"
	 * >Describe</a>to get the metadata of the report.
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param reportId
	 *            the ID of the report to inspect
	 * @return a JSON structure in the form of Map
	 * @throws Exception
	 */	
	@SuppressWarnings("unchecked")
	public Map<String, ?> describeReport(SalesforceCredentials creds, String reportId) throws Exception {
		final Map<String, Object> headers = new HashMap<String, Object>(3){
			{
				put(HEADER_CREDENTIALS, creds);
				put("includeDetails", false);
				put("reportId", reportId);
			}
		};
		Map<String, ?> instanceList = (Map<String, ?>) template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_CREATE_REPORT_INSTANCE, reportId, headers);
		Assert.notNull(instanceList, "Report instance list must not be empty.");
		String instanceId = (String) instanceList.get("id");
		int retry = 3;
		Map<String, ?> metadata = null;
		do {
			metadata = SalesforceObjectHelper.readJsonFromStream(template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_INSTANCE_DATA, instanceId, headers, InputStream.class));
			if (metadata != null && metadata.containsKey("factMap") && metadata.containsKey("reportExtendedMetadata")) {
				retry = 0; // immediate exit
			} else {
				Thread.sleep(500);
				retry--;
			}
		} while (retry > 0);
		debug(metadata);
		return metadata;
	}

	/**
	 * Trigger the Salesforce Reporting API <a href=
	 * "https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_getreportrundata.htm"
	 * >Execute Sync</a>to synchronously retrieve the data of the report.
	 * 
	 * @param creds
	 *            Salesforce credentials
	 * @param reportId
	 *            the ID of the report to get
	 * @param instanceId
	 *            the ID of the instance containing the report data
	 * @return an input stream containing the JSON structure of the report data
	 * @throws Exception
	 */		
	public InputStream getReportData(SalesforceCredentials creds, String reportId, String instanceId) throws Exception {
		Map<String, Object> headers = new HashMap<String, Object>(2){
			{
				put(HEADER_CREDENTIALS, creds);
				put("reportId", reportId);
			}
		};
		return template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_INSTANCE_DATA, 
				instanceId, headers, InputStream.class);
	}
	
	
	/**
	 * For debugging purpose only. Print out the pretty JSON data of the object
	 * 
	 * @param obj
	 * @throws Exception
	 */
	private void debug(Object obj) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(SalesforceObjectHelper.print(obj));
		}
	}
}
