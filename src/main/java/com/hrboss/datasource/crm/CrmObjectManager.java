package com.hrboss.datasource.crm;

import java.util.List;

import com.hrboss.model.DataSource;

public interface CrmObjectManager {

	Object getObjectData(DataSource dataSource, String objectName, String id) throws Exception;
	
	List<?> fetchRemoteObjects(DataSource dataSource, String objectName, int limit, int offset) throws Exception;
	
}
