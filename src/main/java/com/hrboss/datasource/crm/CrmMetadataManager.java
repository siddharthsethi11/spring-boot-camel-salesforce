package com.hrboss.datasource.crm;

import java.util.List;

import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;

public interface CrmMetadataManager {

	boolean testConnection(DataSource dataSource) throws Exception;

	DataSource save(DataSource dataSource) throws Exception;

	List<DataSet> buildObjectsMetadata(DataSource dataSource, String objectName)
			throws Exception;

}
