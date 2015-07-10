package com.github.deeprot.service;

import java.util.List;

import com.github.deeprot.model.DataSet;
import com.github.deeprot.model.DataSource;
import com.github.deeprot.model.RawData;

/**
 * Interface for all the CRM integrations: Salesforce, SAP, ...
 * 
 * @author bruce.nguyen
 *
 */
public interface CrmDatasourceManager {

	public static final String DSFIELD_SF_REPORTID = "SF_REPORT_ID";
	public static final String DSFIELD_SF_DSTYPE = "SF_DATASET_TYPE";
	
	
	/**
	 * Test the connection to the CRM system
	 * 
	 * @param dataSource
	 *            the DTO containing the credentials
	 * @return true/false
	 * @throws Exception
	 */
	boolean testConnection(DataSource dataSource) throws Exception;

	/**
	 * Save the meta-data of all the objects used within the CRM system. Also
	 * count the number of all the items of each object type.
	 * 
	 * @param dataSource
	 *            the DTO containing the credentials
	 * @return the persisted object
	 * @throws Exception
	 */
	DataSource save(DataSource dataSource) throws Exception;

	/**
	 * Build the meta-data of all the objects used within the CRM system and
	 * count the number of objects per type.
	 * 
	 * @param dataSource
	 *            the DTO containing the credentials
	 * @param objectName
	 *            if empty, get the meta-data of all the object types
	 * @return the list of meta-data info and the number of objects per type
	 * @throws Exception
	 */
	List<DataSet> buildObjectsMetadata(DataSource dataSource, String objectName)
			throws Exception;
	
	/**
	 * Get all objects data regarding to an object type. Pagination supported
	 * 
	 * @param dataSource
	 *            the DTO containing the credentials
	 * @param dataSet
	 *            the object type
	 * @param limit
	 *            paging limit
	 * @param offset
	 *            paging offset
	 * @return objects data in JSON format
	 * @throws Exception
	 */
	List<RawData> getObjectsData(DataSource dataSource, DataSet dataSet, int limit, int offset)
			throws Exception;
}