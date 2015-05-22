package com.hrboss.datasource.crm.impl;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.FailedToCreateProducerException;
import org.springframework.beans.factory.annotation.Autowired;

import com.hrboss.datasource.crm.CrmMetadataManager;
import com.hrboss.model.DataSource;

public abstract class AbstractMetadataManagerCamelImpl implements
		CrmMetadataManager {

	@Autowired
	protected CamelContext camelContext;

	@Override
	public boolean testConnection(DataSource dataSource) throws Exception {
		try {
			return verifyCredentials(dataSource);
		} catch (Exception e) {
			if (e instanceof CamelExecutionException) {
				FailedToCreateProducerException camelEx = ((CamelExecutionException) e).getExchange().getException(FailedToCreateProducerException.class);
				throw new Exception(camelEx.getMessage());
			}
		}
		return false;
	}

	@Override
	public DataSource save(DataSource dataSource) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	protected abstract boolean verifyCredentials(DataSource dataSource)
			throws Exception;
}
