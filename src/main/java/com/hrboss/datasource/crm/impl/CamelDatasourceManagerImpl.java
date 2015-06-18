package com.hrboss.datasource.crm.impl;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.hrboss.integration.camel.SalesforceProcessor;
import com.hrboss.integration.camel.dto.QueryRecords;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.helper.SalesforceObjectHelper;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;
import com.hrboss.model.ColumnFieldMetadata;
import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;
import com.hrboss.model.DataType;
import com.hrboss.model.RawData;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Concrete implementation of
 * {@link com.hrboss.cxoboss.datasource.crm.CrmDatasourceManager} using <a
 * href="http://camel.apache.org/">Camel Integration framework</a>. <br/>
 * Current CRM system supported:
 * <ul>
 * <li>Salesforce (<a href="http://camel.apache.org/salesforce.html">link</a>)</li>
 * </ul>
 * 
 * <br/>
 * For a full list of supported components, visit <a
 * href="http://camel.apache.org/components.html">here</a>.
 * 
 * @author bruce.nguyen
 *
 */
@Service
@Lazy
public class CamelDatasourceManagerImpl extends AbstractDatasourceManagerCamelImpl { 
	private static final Logger LOG = LoggerFactory.getLogger(CamelDatasourceManagerImpl.class);
	
	private static final String DSFIELD_SF_REPORTID = "SF_REPORT_ID";
	private static final String DSFIELD_SF_DSTYPE = "SF_DATASET_TYPE";
	
	private static final int THREAD_POOL_SIZE = 100;
	
	@Autowired
	SalesforceProcessor salesforceProcessor;
	
	@Override
	protected boolean verifyCredentials(DataSource dataSource) throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			Collection<Version> versions = salesforceProcessor.getVersions(SalesforceLoginConfigHelper.getCredentials(dataSource));
			return (versions != null && versions.size() > 0);
		default:
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.hrboss.cxoboss.datasource.crm.CrmDatasourceManager#buildObjectsMetadata(com.hrboss.cxoboss.model.DataSource, java.lang.String)
	 */
	@Override
	public List<DataSet> buildObjectsMetadata(DataSource dataSource,
			String objectName) throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			try {
				long duration = System.currentTimeMillis();
				ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
				
				final DataSet template = new DataSet();
				// copy information from DataSource
				//dataset.setDataSourceId(dataSource.getId());
				//dataset.setContainerId(dataSource.getContainerId());
				//dataset.setOwnerId(dataSource.getOwner());
				//dataset.setRoles(dataSource.getRoles());
				
				List<DataSet> objectDS = salesforceProcessor.getObjectTypes(creds, objectName).stream()
						.filter(sObj -> sObj.isQueryable() && sObj.isRetrieveable())
						.map(sObj -> {
							DataSet dataset = (DataSet) template.clone();
							dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.OBJECT);
							dataset.setName(sObj.getName());
							return dataset;
						})
						.collect(Collectors.toList());
				LOG.info("Getting all Salesforce object types completely takes {} m-seconds.", System.currentTimeMillis() - duration);
				duration = System.currentTimeMillis();
				List<DataSet> reportDS = salesforceProcessor.getReportTypes(creds, objectName).entrySet().stream()
						.map(map -> {
							DataSet dataset = (DataSet) template.clone();
							dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.REPORT);
							dataset.put(DSFIELD_SF_REPORTID, map.getKey());
							dataset.setName(map.getValue());
							return dataset;
						})
						.collect(Collectors.toList());
				LOG.info("Getting all Salesforce report types completely takes {} m-seconds.", System.currentTimeMillis() - duration);
				
				List<CompletableFuture<DataSet>> objectDSFutures = objectDS.stream()
						.map(dataset -> CompletableFuture.supplyAsync(() -> getObjectMetadata(creds, dataset), executor))
						.map(dataSetFuture -> (CompletableFuture<DataSet>) dataSetFuture.thenApply(dataset -> countObjectRecordset(creds, dataset)))
						.collect(Collectors.<CompletableFuture<DataSet>>toList());
				// FORBIDDEN : Analytics API can't process the request because it can accept only as many as 20 requests at a time to run reports synchronousl​y
				List<CompletableFuture<DataSet>> reportDSFutures = reportDS.stream()
						.map(dataset -> CompletableFuture.supplyAsync(() -> getReportMetadata(creds, dataset), Executors.newFixedThreadPool(15)))
						.collect(Collectors.<CompletableFuture<DataSet>>toList());
				objectDSFutures.addAll(reportDSFutures);
				
				// no new tasks will be accepted
				executor.shutdown();
				// blocks until all tasks have completed execution
				executor.awaitTermination(3, TimeUnit.SECONDS);
				LOG.info("Getting Salesforce metadata for {"
						+ (objectName == null ? "ALL objects" : objectName) + "} takes {"
						+ (System.currentTimeMillis() - duration) + "} mili-seconds.");
				return expectAllDone(objectDSFutures).get().parallelStream()
						.filter(dataset -> dataset.getRowCount() > 0)
						.collect(Collectors.toList());
			} catch (Exception e) {
				throwRootCause(e);
			}
		default:
			return null;
		}
	}
	
	/*
	 * Private (blocking) method to get the description of a SF object 
	 */
	private DataSet getObjectMetadata(SalesforceCredentials creds, DataSet dataSet)  {
		Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataSet.get(DSFIELD_SF_DSTYPE) == SalesforceProcessor.DatasetType.OBJECT, 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataSet.get(DSFIELD_SF_DSTYPE)));
		
		long duration = System.currentTimeMillis();
		final BasicDBObject columnMetadataList = new BasicDBObject();
		final BasicDBList primaryKeys = new BasicDBList();
		final BasicDBObject foreignKeys = new BasicDBObject();
		
		try {
			SObjectDescription oDesc = salesforceProcessor.describeObject(creds, dataSet.getName());
			oDesc.getFields().stream().forEach(f -> {
				//LOG.debug("Storing field: " + f.getName());
				ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
				columnMetadata.setAlias(f.getLabel());
				columnMetadata.setType(DataType.guess(f.getType()).getName());
				columnMetadataList.put(f.getName(), columnMetadata);
				if ("id".equals(f.getType())) {
					primaryKeys.add(f.getName());
				}
			});
			dataSet.setOriginalFields(columnMetadataList);
			dataSet.setHasPrimaryKey(primaryKeys.size() > 0);
			dataSet.setPrimaryKeys(primaryKeys);
			oDesc.getChildRelationships().stream().forEach(child -> {
				//LOG.debug("Storing child object: " + child.getChildSObject());
				foreignKeys.put(child.getField(), child.getChildSObject());
			});
			dataSet.setHasForeignKey(foreignKeys.size() > 0);
			dataSet.setForeignKeys(foreignKeys);
		} catch (Exception e) {
			LOG.warn("Failed to describe Salesforce object %s.", dataSet.getName(), e);
		}
		LOG.info("Describing object {} takes {} m-seconds.", dataSet.getName(), System.currentTimeMillis() - duration);
		return dataSet;
	}
	
	/*
	 * Private (blocking) method to count the number of records of a SF object
	 * 
	 */
	private DataSet countObjectRecordset(SalesforceCredentials creds, DataSet dataset) {
		Assert.notNull(dataset.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataset.get(DSFIELD_SF_DSTYPE) == SalesforceProcessor.DatasetType.OBJECT, 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataset.get(DSFIELD_SF_DSTYPE)));
		
		long duration = System.currentTimeMillis();
		int rowCount = -1;
		try {
			rowCount = salesforceProcessor.countObject(creds, dataset.getName());
			dataset.setRowCount(rowCount);
		} catch (Exception e) {
			LOG.warn("Failed to count Salesforce object {}.", dataset.getName(), e);
		}
		LOG.info("Total number of object {} is {} record, counting operation takes {} m-seconds.", dataset.getName(), rowCount, System.currentTimeMillis() - duration);
		return dataset;
	}
	
	/*
	 * Private (blocking) method to get the description of a SF report 
	 */
	private DataSet getReportMetadata(SalesforceCredentials creds, DataSet dataSet)  {
		Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataSet.get(DSFIELD_SF_DSTYPE) == SalesforceProcessor.DatasetType.REPORT, 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataSet.get(DSFIELD_SF_DSTYPE)));
		Assert.notNull(dataSet.get(DSFIELD_SF_REPORTID), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_REPORTID));
		
		long duration = System.currentTimeMillis();
		final BasicDBObject columnMetadataList = new BasicDBObject();
		DocumentContext document = null;
		Map<String, Object> reportMetadata = null;
		try {
			reportMetadata = salesforceProcessor.describeReport(creds, (String) dataSet.get(DSFIELD_SF_REPORTID));
			Configuration conf = Configuration.defaultConfiguration();
			conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
			document = JsonPath.using(conf).parse(reportMetadata);
			int count = document.read("$['factMap']['T!T']['aggregates'][(@.length-1)]['value']");
			Map<String, Map<String, Object>> columnInfos = document.read("$.reportExtendedMetadata.detailColumnInfo");
			columnInfos.entrySet().stream().forEach(entry -> {
				ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
				columnMetadata.setAlias((String) entry.getValue().get("label"));
				columnMetadata.setType(DataType.guess((String) entry.getValue().get("dataType")).getName());
				columnMetadataList.put(entry.getKey(), columnMetadata);
			});
			dataSet.setOriginalFields(columnMetadataList);
			dataSet.setHasPrimaryKey(false);
			dataSet.setHasForeignKey(false);
			dataSet.setRowCount(count);
		} catch (Exception e) {
			LOG.warn("Failed to describe Salesforce report: {}", SalesforceObjectHelper.print(reportMetadata), e);
		}
		LOG.info("Describing report {} takes {} m-seconds.", dataSet.getName(), System.currentTimeMillis() - duration);
		return dataSet;
	}
	
	/*
	 * Utility to get the result of a list of ALL-COMPLETED future tasks
	 */
	private <T> CompletableFuture<List<T>> expectAllDone(List<CompletableFuture<T>> futures) {
	    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
	    return allDoneFuture.thenApply(v -> futures.stream().
	                    map(future -> future.join()).
	                    collect(Collectors.<T>toList())
	    );
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.hrboss.cxoboss.datasource.crm.CrmDatasourceManager#getObjectsData(com.hrboss.cxoboss.model.DataSource, com.hrboss.employeeboss.model.mongodb.DataSet, int, int)
	 */
	@Override
	public List<RawData> getObjectsData(DataSource dataSource, DataSet dataSet, int limit, int offset)
			throws Exception {
		List<RawData> resultSet = null;
		switch (dataSource.getType()) {
		case FILE:
			break;
		case DATABASE:
			break;
		case SALESFORCE:
			Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
					String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			String objectName = dataSet.getName();
			SalesforceProcessor.DatasetType dsType = (SalesforceProcessor.DatasetType) dataSet.get(DSFIELD_SF_DSTYPE);
			try {
				long duration = System.currentTimeMillis();
				Collection<?> sfDataset = null;
				switch (dsType) {
				case OBJECT:
					SObjectDescription oDesc = salesforceProcessor.describeObject(creds, objectName);
					List<String> fields = null;
					QueryRecords<?> resultObj = null;
					if (offset > 0) {
						fields = oDesc.getFields().stream()
								.map(f -> f.getName()).collect(Collectors.toList());
						resultObj = salesforceProcessor.getObjectWindows(creds, objectName, fields, limit, offset);
					} else {
						fields = oDesc.getFields().stream()
								// FUNCTIONALITY_NOT_ENABLED: Selecting compound data not supported in Bulk Query
								.filter(f -> !f.getSoapType().startsWith("urn:"))
								.map(f -> f.getName()).collect(Collectors.toList());
						resultObj = salesforceProcessor.bulkQueryObjects(creds, objectName, fields, limit);
					}
					if (resultObj != null && resultObj.getTotalSize() > 0) {
						sfDataset = resultObj.getRecords();
						
					}
					break;
				case REPORT:
					Assert.notNull(dataSet.get(DSFIELD_SF_REPORTID), 
							String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_REPORTID));
					InputStream reportDataStream = salesforceProcessor.getReportData(creds, (String) dataSet.get(DSFIELD_SF_REPORTID));
					//Path path = new File("D:\\tmp\\reportdata.json").toPath();
					//Files.copy(reportDataStream, path, StandardCopyOption.REPLACE_EXISTING);
					//InputStream reportDataStream = new FileInputStream(new File("D:\\data\\salesforce\\all-contacts-list-report.data.json"));
					sfDataset = SalesforceObjectHelper.readSalesforceReportDataStreamToMongoObject(reportDataStream);
					break;
				}
				if (sfDataset != null) {
					resultSet = sfDataset.parallelStream().map(o -> {
						RawData record = new RawData();
						record.setDatasetId(dataSet.getId());
						record.put("data", o);
						return record;
					}).collect(Collectors.toList());
				}
				duration = System.currentTimeMillis() - duration;
				LOG.info("Getting Salesforce data for dataset {" + objectName
						+ "} takes {" + duration + "} mili-seconds.");
			} catch (Exception e) {
				throwRootCause(e);
			}
		}
		return resultSet;
	}
	
	/**
	 * Throw the root cause in the exception stack
	 * 
	 * @param e
	 * @throws Exception
	 */
	private void throwRootCause(Exception e) throws Exception {
		if (e instanceof CamelExecutionException) {
			Throwable t = ((CamelExecutionException) e).getCause();
			while (t.getCause() != null) {
				t = t.getCause();
			}
			throw new Exception(t);
		} else {
			throw e;
		}
	}

	public List<String> getUnsupportedObjects(DataSource dataSource) throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			return salesforceProcessor.getBulkQueryUnsupportedObjects(creds);
		default:
			return null;
		}
	}
}
