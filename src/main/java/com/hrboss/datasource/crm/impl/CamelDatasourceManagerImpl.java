package com.hrboss.datasource.crm.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.component.salesforce.api.dto.SObject;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.hrboss.integration.camel.SalesforceProcessor;
import com.hrboss.integration.camel.dto.QueryRecords;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;
import com.hrboss.model.ColumnFieldMetadata;
import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;
import com.hrboss.model.RawData;
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
	
	@Autowired
	SalesforceProcessor salesforceProcessor;
	
	@Override
	protected boolean verifyCredentials(DataSource dataSource) throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			try {
				Collection<Version> versions = salesforceProcessor.getVersions(SalesforceLoginConfigHelper.getCredentials(dataSource));
				return (versions != null && versions.size() > 0);
			} catch (Exception e) {
				throwRootCause(e);
			}
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
			// TODO: should make the whole process asynchronous
			try {
				Collection<SObject> sObjects = salesforceProcessor.getObjectTypes(creds, objectName);
				final List<DataSet> dataSets = new ArrayList<DataSet>(sObjects.size());
				sObjects.parallelStream().forEach( sObj -> {
					try { // try to get as many objects as possible
						LOG.debug("Investigating object type: " + sObj.getName());
						final DataSet dataSet = new DataSet();
						// copy information from DataSource
						// dataSet.setDataSourceId(dataSource.getId());
						// dataSet.setContainerId(dataSource.getContainerId());
						// dataSet.setOwnerId(dataSource.getOwner());
						// dataSet.setRoles(dataSource.getRoles());
						
						final BasicDBObject columnMetadataList = new BasicDBObject();
						final BasicDBList primaryKeys = new BasicDBList();
						final BasicDBObject foreignKeys = new BasicDBObject();
						dataSet.setName(sObj.getName());
						SObjectDescription oDesc = salesforceProcessor.describeObject(creds, sObj.getName());
						oDesc.getFields().stream().forEach(f -> {
							LOG.debug("Storing field: " + f.getName());
							ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
							columnMetadata.setAlias(f.getLabel());
							columnMetadata.setType(f.getType());
							columnMetadataList.put(f.getName(), columnMetadata);
							if ("id".equals(f.getType())) {
								primaryKeys.add(f.getName());
							}
						});
						dataSet.setOriginalFields(columnMetadataList);
						dataSet.setHasPrimaryKey(primaryKeys.size() > 0);
						dataSet.setPrimaryKeys(primaryKeys);
						oDesc.getChildRelationships().stream().forEach(child -> {
							LOG.debug("Storing child object: " + child.getChildSObject());
							foreignKeys.put(child.getField(), child.getChildSObject());
						});
						dataSet.setHasForeignKey(foreignKeys.size() > 0);
						dataSet.setForeignKeys(foreignKeys);
						int rowCount = salesforceProcessor.countObject(creds, sObj.getName());
						LOG.debug("Storing row count: " + rowCount);
						dataSet.setRowCount(rowCount);
						dataSets.add(dataSet);
					} catch (Exception e) {
						// TODO: should let user knows about failed objects
						LOG.warn("Failed to get meta-data for object {"
								+ sObj.getName() + "} due to: "
								+ e.getCause().getMessage());
					}
				});
				return dataSets;
			} catch (Exception e) {
				throwRootCause(e);
			}
		default:
			return null;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.hrboss.cxoboss.datasource.crm.CrmDatasourceManager#getObjectsData(com.hrboss.cxoboss.model.DataSource, com.hrboss.employeeboss.model.mongodb.DataSet, int, int)
	 */
	@Override
	public List<RawData> getObjectsData(DataSource dataSource, DataSet dataSet, int limit, int offset)
			throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			String objectName = dataSet.getName();
			try {
				SObjectDescription oDesc = salesforceProcessor.describeObject(creds, objectName);
				final List<String> fields = oDesc.getFields().stream()
						// FUNCTIONALITY_NOT_ENABLED: Selecting compound data not supported in Bulk Query
						.filter(f -> !f.getSoapType().startsWith("urn:"))
						.map(f -> f.getName()).collect(Collectors.toList());
				QueryRecords<?> resultObj = null;
				if (offset > 0) {
					resultObj = salesforceProcessor.getObjectWindows(creds, objectName, fields, limit, offset);
				} else {
					resultObj = salesforceProcessor.bulkQueryObjects(creds, objectName, fields, limit);
				}
				if (resultObj != null && resultObj.getTotalSize() > 0) {
					return resultObj.getRecords().parallelStream().map(o -> {
						RawData record = new RawData();
						record.setDatasetId(dataSet.getId());
						record.put("data", o);
						return record;
					}).collect(Collectors.toList());
				}
			} catch (Exception e) {
				throwRootCause(e);
			}
		default:
			return null;
		}
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
