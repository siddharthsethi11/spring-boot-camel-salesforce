package com.github.deeprot.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
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

import com.github.deeprot.integration.camel.SalesforceProcessor;
import com.github.deeprot.integration.camel.dto.QueryRecords;
import com.github.deeprot.integration.camel.dto.SalesforceCredentials;
import com.github.deeprot.integration.helper.SalesforceLoginConfigHelper;
import com.github.deeprot.integration.helper.SalesforceObjectHelper;
import com.github.deeprot.model.ColumnFieldMetadata;
import com.github.deeprot.model.DataSet;
import com.github.deeprot.model.DataSource;
import com.github.deeprot.model.DataType;
import com.github.deeprot.model.DatasourceType;
import com.github.deeprot.model.RawData;
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
	
	private static final int THREAD_POOL_SIZE = 100;
	
	@Autowired
	SalesforceProcessor salesforceProcessor;
	
	@Override
	protected boolean verifyCredentials(DataSource dataSource) throws Exception {
		switch (DatasourceType.valueOf(dataSource.getType())) {
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
		switch (DatasourceType.valueOf(dataSource.getType())) {
		case SALESFORCE:
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			try {
				ExecutorService executor = Executors.newCachedThreadPool();
				final DataSet template = new DataSet();
				// copy information from DataSource
				template.setDataSourceId(dataSource.getId());
				template.setContainerId(dataSource.getContainerId());
				template.setOwnerId(dataSource.getOwner());
				template.setRoles(dataSource.getRoles());

				long duration = System.currentTimeMillis();
				CompletableFuture<List<DataSet>> objectDSFuture = CompletableFuture.supplyAsync(() -> salesforceProcessor.getObjectTypes(creds, objectName).stream()
						.filter(sObj -> sObj.isQueryable() && sObj.isRetrieveable())
						.map(sObj -> {
							DataSet dataset = (DataSet) template.clone();
							dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.OBJECT.toString());
							dataset.setName(sObj.getName());
							return dataset;
						})
						.collect(Collectors.toList()), executor);
				CompletableFuture<List<DataSet>> reportDSFuture = CompletableFuture.supplyAsync(() -> salesforceProcessor.getReportTypes(creds, objectName).stream()
						.map(map -> {
							DataSet dataset = (DataSet) template.clone();
							dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.REPORT.toString());
							dataset.put(DSFIELD_SF_REPORTID, map.get("Id"));
							dataset.setName((String)map.get("Name"));
							return dataset;
						})
						.collect(Collectors.toList()), executor);
				CompletableFuture.allOf(objectDSFuture, reportDSFuture).join();
				LOG.info("Getting Salesforce object & report types completely takes {} m-seconds.", System.currentTimeMillis() - duration);
				
				List<CompletableFuture<DataSet>> objectMetaDSFutures = objectDSFuture.get().stream()
						.map(dataset -> CompletableFuture.supplyAsync(() -> getObjectMetadata(creds, dataset), executor))
						.map(dataSetFuture -> (CompletableFuture<DataSet>) dataSetFuture.thenApply(dataset -> countObjectRecordset(creds, dataset)))
						.collect(Collectors.<CompletableFuture<DataSet>>toList());
				List<CompletableFuture<DataSet>> reportMetaDSFutures = reportDSFuture.get().stream()
						.map(dataset -> CompletableFuture.supplyAsync(() -> getReportMetadata(creds, dataset), executor))
						.collect(Collectors.<CompletableFuture<DataSet>>toList());
				// combine two list
				objectMetaDSFutures.addAll(reportMetaDSFutures);
				// no new tasks will be accepted
				executor.shutdown();
				// blocks until all tasks have completed execution
				executor.awaitTermination(3, TimeUnit.SECONDS);
				List<DataSet> objectMetaList = expectAllDone(objectMetaDSFutures).get().parallelStream()
					.filter(dataset -> dataset.getRowCount() != 0)
					.collect(Collectors.toList());
				
				LOG.info("Getting Salesforce metadata for "
						+ (objectName == null ? "ALL objects and reports" : objectName) + " takes {"
						+ (System.currentTimeMillis() - duration) + "} mili-seconds.");
				return objectMetaList;
			} catch (Exception e) {
				throwRootCause(e);
			}
		default:
			return null;
		}
	}
	
	@Deprecated
	public List<DataSet> buildSalesforceObjectsMetadata(final DataSet template, final SalesforceCredentials creds, final String objectName, 
			ExecutorService executor, boolean withRowCount) throws Exception {
		try {
			long duration = System.currentTimeMillis();
			List<DataSet> objectDS = salesforceProcessor.getObjectTypes(creds, objectName).stream()
					.filter(sObj -> sObj.isQueryable() && sObj.isRetrieveable())
					.map(sObj -> {
						DataSet dataset = (DataSet) template.clone();
						dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.OBJECT.toString());
						dataset.setName(sObj.getName());
						return dataset;
					})
					.collect(Collectors.toList());
			LOG.info("Getting all Salesforce object types completely takes {} m-seconds.", System.currentTimeMillis() - duration);
			
			duration = System.currentTimeMillis();
			List<CompletableFuture<DataSet>> objectDSFutures = null;
			if (withRowCount) {
				objectDSFutures = objectDS.stream()
					.map(dataset -> CompletableFuture.supplyAsync(() -> getObjectMetadata(creds, dataset), executor))
					.map(dataSetFuture -> (CompletableFuture<DataSet>) dataSetFuture.thenApply(dataset -> countObjectRecordset(creds, dataset)))
					.collect(Collectors.<CompletableFuture<DataSet>>toList());
			} else {
				objectDSFutures = objectDS.stream()
					.map(dataset -> CompletableFuture.supplyAsync(() -> getObjectMetadata(creds, dataset), executor))
					.collect(Collectors.<CompletableFuture<DataSet>>toList());
			}
			// no new tasks will be accepted
			executor.shutdown();
			// blocks until all tasks have completed execution
			executor.awaitTermination(3, TimeUnit.SECONDS);
			List<DataSet> objectMetaDS = expectAllDone(objectDSFutures).get().parallelStream()
				.collect(Collectors.toList());

			LOG.info("Getting Salesforce metadata for {"
					+ (objectName == null ? "ALL objects" : objectName) + "} takes {"
					+ (System.currentTimeMillis() - duration) + "} mili-seconds.");
			return objectMetaDS;
		} catch (Exception e) {
			throwRootCause(e);
		}
		return null;
	}

	@Deprecated
	public List<DataSet> buildSalesforceReportsMetadata(final DataSet template, final SalesforceCredentials creds, final String objectName, 
		ExecutorService executor) throws Exception {
		try {
			long duration = System.currentTimeMillis();
			List<DataSet> reportDS = salesforceProcessor.getReportTypes(creds, objectName).stream()
					.map(map -> {
						DataSet dataset = (DataSet) template.clone();
						dataset.put(DSFIELD_SF_DSTYPE, SalesforceProcessor.DatasetType.REPORT.toString());
						dataset.put(DSFIELD_SF_REPORTID, map.get("Id"));
						dataset.setName((String)map.get("Name"));
						return dataset;
					})
					.collect(Collectors.toList());
			LOG.info("Getting all Salesforce report types completely takes {} m-seconds.", System.currentTimeMillis() - duration);
			List<CompletableFuture<DataSet>> reportMetaDSFutures = reportDS.stream()
					.map(dataset -> CompletableFuture.supplyAsync(() -> getReportMetadata(creds, dataset), executor))
					.collect(Collectors.<CompletableFuture<DataSet>>toList());
			// no new tasks will be accepted
			executor.shutdown();
			// blocks until all tasks have completed execution
			executor.awaitTermination(3, TimeUnit.SECONDS);
			List<DataSet> reportMetaList = expectAllDone(reportMetaDSFutures).get().parallelStream()
				.collect(Collectors.toList());
			/*List<DataSet> reportMetaList = new CopyOnWriteArrayList<DataSet>();
			List<DataSet> failedList = new CopyOnWriteArrayList<DataSet>();
			int retry = 1;
			do {
				failedList.clear();
				failedList = batchGetReportMetadata(reportDS, creds, executor, reportMetaList);
				reportDS.clear();
				reportDS.addAll(failedList);
				retry --;
			} while (retry >= 0 && failedList.size() > 0);*/

			LOG.info("Getting Salesforce metadata for {"
					+ (objectName == null ? "ALL reports" : objectName) + "} takes {"
					+ (System.currentTimeMillis() - duration) + "} mili-seconds.");
			return reportMetaList;
		} catch (Exception e) {
			throwRootCause(e);
		}
		return null;
	}	
	
	@Deprecated
	private List<DataSet> batchGetReportMetadata(List<DataSet> sourceList, SalesforceCredentials creds, ExecutorService executor, List<DataSet> composedList) {
		List<DataSet> failedList = new ArrayList<DataSet>();
		CompletableFuture[] reportDSFutures = sourceList.stream()
				.map(dataset -> CompletableFuture.supplyAsync(() -> {
					DataSet newDS = null;
					try {
						newDS = getReportMetadata(creds, dataset);
						composedList.add(newDS);
					} catch (Exception e) {
						failedList.add(dataset);
						try {
							throwRootCause(e);
						} catch (Exception rootcause) {
							LOG.error(String.format("Failed getting metadata for report {%s}", dataset.getName()), rootcause);
						}
					}
					return newDS;
				}, executor))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(reportDSFutures).join();
		executor.shutdown();
		LOG.debug("Successfully getting metdata for #{} additional reports out of #{}.", reportDSFutures.length - failedList.size(), sourceList.size());
		return failedList;
	}	
	
	/*
	 * Private (blocking) method to get the description of a SF object 
	 * NOTE: applicable only for Salesforce Objects
	 */
	private DataSet getObjectMetadata(SalesforceCredentials creds, DataSet dataSet)  {
		Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataSet.get(DSFIELD_SF_DSTYPE).equals(SalesforceProcessor.DatasetType.OBJECT.toString()), 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataSet.get(DSFIELD_SF_DSTYPE)));
		
		long duration = System.currentTimeMillis();
		final BasicDBObject columnMetadataList = new BasicDBObject();
		final BasicDBList primaryKeys = new BasicDBList();
		final BasicDBObject foreignKeys = new BasicDBObject();
		
		try {
			SObjectDescription oDesc = salesforceProcessor.describeObject(creds, dataSet.getName());
			oDesc.getFields().stream().forEach(f -> {
				StringBuilder key = SalesforceObjectHelper.normalizeFieldname(f.getName());
				//LOG.debug("Storing field: " + f.getName());
				ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
				columnMetadata.setAlias(f.getLabel());
				columnMetadata.setType(DataType.guess(f.getType()).getName());
				columnMetadataList.put(key.toString(), columnMetadata);
				if ("id".equals(f.getType())) {
					primaryKeys.add(key.toString());
				}
			});
			dataSet.setOriginalFields(columnMetadataList);
			dataSet.setHasPrimaryKey(primaryKeys.size() > 0);
			dataSet.setPrimaryKeys(primaryKeys);
			oDesc.getChildRelationships().stream().forEach(child -> {
				//LOG.debug("Storing child object: " + child.getChildSObject());
				foreignKeys.put(SalesforceObjectHelper.normalizeFieldname(child.getField()).toString(), child.getChildSObject());
			});
			dataSet.setHasForeignKey(foreignKeys.size() > 0);
			dataSet.setForeignKeys(foreignKeys);
			LOG.info("Describing object {} takes {} m-seconds.", dataSet.getName(), System.currentTimeMillis() - duration);
		} catch (Exception e) {
			LOG.warn(String.format("Failed to describe Salesforce object {%s}.", dataSet.getName()), e);
		}
		return dataSet;
	}
	
	/*
	 * Private (blocking) method to count the number of records of a SF object
	 * NOTE: applicable only for Salesforce Objects
	 * 
	 */
	private DataSet countObjectRecordset(SalesforceCredentials creds, DataSet dataset) {
		Assert.notNull(dataset.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataset.get(DSFIELD_SF_DSTYPE).equals(SalesforceProcessor.DatasetType.OBJECT.toString()), 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataset.get(DSFIELD_SF_DSTYPE)));
		
		long duration = System.currentTimeMillis();
		int rowCount = -1;
		try {
			rowCount = salesforceProcessor.countObject(creds, dataset.getName());
			dataset.setRowCount(rowCount);
			LOG.info(String.format("Total number of object {%s} is {%d} record, counting operation takes {%d} m-seconds.", 
					dataset.getName(), rowCount, System.currentTimeMillis() - duration));
		} catch (Exception e) {
			LOG.warn(String.format("Failed to count Salesforce object {%s}.", dataset.getName()), e);
		}
		return dataset;
	}
	
	/*
	 * Private (blocking) method to get the description of a SF report using Json Path
	 * NOTE: applicable only for Salesforce Reports
	 */
	private DataSet getReportMetadata(SalesforceCredentials creds, DataSet dataSet) { //throws Exception {
		Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
		Assert.isTrue(dataSet.get(DSFIELD_SF_DSTYPE).equals(SalesforceProcessor.DatasetType.REPORT.toString()), 
				String.format("Method is not applicable for Salesforce DS type {%s}", dataSet.get(DSFIELD_SF_DSTYPE)));
		Assert.notNull(dataSet.get(DSFIELD_SF_REPORTID), 
				String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_REPORTID));
		
		long duration = System.currentTimeMillis();
		final BasicDBObject columnMetadataList = new BasicDBObject();
		DocumentContext document = null;
		Map<String, ?> reportMetadata = null;
		try {
			reportMetadata = salesforceProcessor.describeReport(creds, (String) dataSet.get(DSFIELD_SF_REPORTID));
			Configuration conf = Configuration.defaultConfiguration();
			conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
			document = JsonPath.using(conf).parse(reportMetadata);
			int count = SalesforceObjectHelper.readReportMetadata(document, columnMetadataList);
			dataSet.setOriginalFields(columnMetadataList);
			dataSet.setHasPrimaryKey(false);
			dataSet.setHasForeignKey(false);
			dataSet.setRowCount(count);
			LOG.info("Describing report {} takes {} m-seconds.", dataSet.getName(), System.currentTimeMillis() - duration);
		} catch (Exception e) {
			LOG.warn(String.format("Failed to describe Salesforce report {%s}.", dataSet.getName()), e);
		}
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
		switch (DatasourceType.valueOf(dataSource.getType())) {
		case FILE:
			break;
		case DATABASE:
			break;
		case SALESFORCE:
			Assert.notNull(dataSet.get(DSFIELD_SF_DSTYPE), 
					String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_DSTYPE));
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			String objectName = dataSet.getName();
			SalesforceProcessor.DatasetType dsType = SalesforceProcessor.DatasetType.valueOf((String)dataSet.get(DSFIELD_SF_DSTYPE));
			try {
				long duration = System.currentTimeMillis();
				Collection<?> sfDataset = null;
				switch (dsType) {
					case REPORT:
						Assert.notNull(dataSet.get(DSFIELD_SF_REPORTID), 
								String.format("Dataset does not contain mandatory custom field {%s}", DSFIELD_SF_REPORTID));
						InputStream reportDataStream = salesforceProcessor.getReportData(creds, (String) dataSet.get(DSFIELD_SF_REPORTID) ,null);
						sfDataset = SalesforceObjectHelper.readSalesforceReportDataStreamToMongoObject(reportDataStream);
						break;
					default:
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
}
