package com.hrboss.integration.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hrboss.datasource.crm.CrmMetadataManager;
import com.hrboss.datasource.crm.CrmObjectManager;
import com.hrboss.integration.helper.ObjectHelper;
import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;
import com.hrboss.model.DatasourceType;

@Controller
public class Front {

	private static final String EMAIL = "bernie.schiemer@hiringboss.com";
	private static final String PASSWORD = "Remember12";
	private static final String CONSUMER_SECRET = "6630656843211848500";
	private static final String CONSUMER_KEY = "3MVG9I1kFE5Iul2BLhYUBv2s5B6ndxx8LPJecj5cBYNkD9DDrqeL3Sm7LQ6REzZ6vb4MvWG9G65rxXYxGLHyr";
	private static final Logger LOG = LoggerFactory.getLogger(Front.class);
	
	static DataSource ds = new DataSource();
	{
		ds.setEmail(EMAIL);
		ds.setPassword(PASSWORD);
		ds.setClientKey(CONSUMER_KEY);
		ds.setClientSecureKey(CONSUMER_SECRET);
		ds.setType(DatasourceType.SALESFORCE);
	}

	@Autowired
	CrmMetadataManager crmDSMgr;
	@Autowired
	CrmObjectManager crmObjMgr;

	@RequestMapping(value = "/login", method = RequestMethod.GET)
	@ResponseBody
	String home() {
		try {
			boolean ok = crmDSMgr.testConnection(ds);
			if (ok) {
				return "Hello " + ds.getEmail() + "!";
			} else {
				return "Failed login, " + ds.getEmail();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return e.getMessage();
		}
	}
	
	@RequestMapping(value = "/{object}/count", method = RequestMethod.GET)
	@ResponseBody
	String count(@PathVariable("object") String objectName) {
		try {
			List<DataSet> dataSet = crmDSMgr.buildObjectsMetadata(ds, objectName);
			return "Number of object {" + objectName + "} : "
					+ dataSet.get(0).getRowCount();
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return e.getMessage();
		}
	}
	
	/*@RequestMapping(value = "/{object}/import", method = RequestMethod.GET)
	@ResponseBody
	String importObject(@PathVariable("object") String objectName) {
		try {
			return "Number of object {" + objectName + "} imported : "
					+ crmObjMgr.fetchRemoteObjects(DatasourceType.SALESFORCE, objectName, 10, 0);
		} catch (Exception e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}*/
	
	@RequestMapping(value = "/{object}/get/{id}", method = RequestMethod.GET)
	@ResponseBody
	String getObject(@PathVariable("object") String objectName, @PathVariable("id") String objectId) {
		try {
			Object o = crmObjMgr.getObjectData(ds, objectName, objectId);
			return ObjectHelper.debug(o);
		} catch (Exception e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}
}
