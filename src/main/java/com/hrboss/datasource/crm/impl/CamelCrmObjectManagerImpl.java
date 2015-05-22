package com.hrboss.datasource.crm.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.hrboss.datasource.crm.CrmObjectManager;
import com.hrboss.integration.camel.SalesforceProcessor;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;
import com.hrboss.model.DataSource;

@Service@Lazy
public class CamelCrmObjectManagerImpl implements CrmObjectManager {

	private static final Logger LOG = LoggerFactory.getLogger(CamelCrmObjectManagerImpl.class);
	
	@Autowired
	SalesforceProcessor salesforceProcessor;
	
	@Override
	public Object getObjectData(DataSource dataSource, String objectName,
			String id) throws Exception {
		switch (dataSource.getType()) {
			case SALESFORCE:
				SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(dataSource);
				SObjectDescription oDesc = salesforceProcessor.describeObject(creds, objectName);
				List<String> fields = new ArrayList<String>(oDesc.getFields().size());
				for (SObjectField f : oDesc.getFields()) {
					fields.add(f.getName());
				}
				return salesforceProcessor.getObject(creds, fields, objectName, id);
		}
		return null;
	}

	@Override
	public List<?> fetchRemoteObjects(DataSource dataSource, String objectName,
			int limit, int offset) throws Exception {
		switch (dataSource.getType()) {
			case SALESFORCE:
				return null;
		}
		return null;
	}

}
