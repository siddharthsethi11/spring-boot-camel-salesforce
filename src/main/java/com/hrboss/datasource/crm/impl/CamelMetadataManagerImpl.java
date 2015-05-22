package com.hrboss.datasource.crm.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.camel.component.salesforce.api.dto.ChildRelationShip;
import org.apache.camel.component.salesforce.api.dto.SObject;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.camel.component.salesforce.api.dto.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.hrboss.integration.camel.SalesforceProcessor;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;
import com.hrboss.model.ColumnFieldMetadata;
import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;
import com.mongodb.BasicDBObject;

@Service @Lazy
public class CamelMetadataManagerImpl extends AbstractMetadataManagerCamelImpl { 

	private static final Logger LOG = LoggerFactory.getLogger(CamelMetadataManagerImpl.class);
	
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

	@Override
	public List<DataSet> buildObjectsMetadata(DataSource dataSource,
			String objectName) throws Exception {
		switch (dataSource.getType()) {
		case SALESFORCE:
			SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
			Collection<SObject> sObjects = salesforceProcessor.getObjectTypes(creds, objectName);
			List<DataSet> dataSets = new ArrayList<DataSet>(sObjects.size());
			for (final SObject sObj : sObjects) {
				try {
					LOG.debug("Investigating object type: " + sObj.getName());
					final DataSet dataSet = new DataSet();
					final BasicDBObject columnMetadataList = new BasicDBObject();
					final BasicDBObject foreignKeys = new BasicDBObject();
	
					SObjectDescription oDesc = salesforceProcessor.describeObject(creds, sObj.getName());
					//oDesc.getFields().stream().forEach(f -> {
					for (SObjectField f : oDesc.getFields()) {
						LOG.debug("\tStoring field: " + f.getName());
						ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
						columnMetadata.setAlias(f.getLabel());
						columnMetadata.setType(f.getType());
						columnMetadataList.put(f.getName(), columnMetadata);
					}
					//});
					dataSet.setOriginalFields(columnMetadataList);
					for (ChildRelationShip child : oDesc.getChildRelationships()) {
					//oDesc.getChildRelationships().stream().forEach(child -> {
						LOG.debug("\tStoring child object: " + child.getChildSObject());
						foreignKeys.put(child.getField(), child.getChildSObject());
					//});
					}
					dataSet.setForeignKeys(foreignKeys);
					int rowCount = salesforceProcessor.countObject(creds, sObj);
					LOG.debug("\tStoring row count: " + rowCount);
					dataSet.setRowCount(rowCount);
					dataSets.add(dataSet);
				} catch (Exception e) {
					// TODO: should let user knows about failed objects
					LOG.warn("Failed to get meta-data for object {"
							+ sObj.getName() + "} due to " + e.getMessage());
				}
			}
			return dataSets;
		default:
			return null;
		}
	}
	
}
