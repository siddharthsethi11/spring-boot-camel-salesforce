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

import com.hrboss.datasource.crm.CrmDatasourceManager;
import com.hrboss.integration.helper.ObjectHelper;
import com.hrboss.model.DataSet;
import com.hrboss.model.DataSource;
import com.hrboss.model.DatasourceType;
import com.hrboss.model.RawData;

@Controller
public class Front {

	private static final String EMAIL = "kennethwpeeples@redhat.com";
	private static final String PASSWORD = "Ke!thluvsmem@re2013";
	private static final String CONSUMER_SECRET = "7483376435449564293";
	private static final String CONSUMER_KEY = "3MVG9fMtCkV6eLhepJuasZZ7OzymyGMNikp2IxdqQ8H1Jx_uWNkSU5gIA9.mxSzlsGoNzEE18cy2XKPARvA_o";
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
	CrmDatasourceManager crmDatasourceManager;

	@RequestMapping(value = "/login", method = RequestMethod.GET)
	@ResponseBody
	String home() {
		try {
			boolean ok = crmDatasourceManager.testConnection(ds);
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

	@RequestMapping(value = "/describe", method = RequestMethod.GET)
	@ResponseBody
	String describeAll() {
		return describe(null);
	}
	
	@RequestMapping(value = "/describe/{object}", method = RequestMethod.GET)
	@ResponseBody
	String describe(@PathVariable("object") String objectName) {
		try {
			long duration = System.nanoTime();
			List<DataSet> dataSet = crmDatasourceManager.buildObjectsMetadata(ds, objectName);
			duration = System.nanoTime() - duration;
			StringBuilder sb = new StringBuilder("Fetch all meta-data from Salesforce account successfully in " + ((int)duration / 1000) + "ms :");
			dataSet.stream().forEach(row -> {
				sb.append("<p>").append("Object {" + row.getName() + "} has " + row.getRowCount() + " item(s).").append("</p>");
				LOG.debug(ObjectHelper.print(row));
			});
			return sb.toString();
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return e.getMessage();
		}
	}
	
	@RequestMapping(value = "/getAll/{object}", method = RequestMethod.GET)
	@ResponseBody
	String getAll(@PathVariable("object") String objectName) {
		return getWindow(objectName, -1, -1);
	}
	
	@RequestMapping(value = "/getAll/{object}/{limit}/{offset}", method = RequestMethod.GET)
	@ResponseBody
	String getWindow(@PathVariable("object") String objectName, @PathVariable("limit") int limit, @PathVariable("offset") int offset) {
		try {
			DataSet dataset = new DataSet();
			dataset.setName(objectName);
			List<RawData> data = crmDatasourceManager.getObjectsData(ds, dataset, limit, offset);
			StringBuilder sb = new StringBuilder();
			data.stream().forEach(row -> {
				sb.append("<p>").append(ObjectHelper.print(row)).append("</p>");
			});
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}
}
