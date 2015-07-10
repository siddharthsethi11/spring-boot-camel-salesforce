package com.github.deeprot.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.deeprot.integration.camel.dto.SalesforceCredentials;
import com.github.deeprot.integration.helper.SalesforceLoginConfigHelper;
import com.github.deeprot.integration.helper.SalesforceObjectHelper;
import com.github.deeprot.model.DataSet;
import com.github.deeprot.model.DataSource;
import com.github.deeprot.model.DatasourceType;
import com.github.deeprot.service.CrmDatasourceManager;
import com.github.deeprot.service.impl.CamelDatasourceManagerImpl;

@RestController
public class Front {
    
	private static final DataSource DEFAULT_DS = new DataSource();
	{
		DEFAULT_DS.setEmail("marketing@hiringboss.com");
		DEFAULT_DS.setDBPassword("HRBoss2015");
		//DEFAULT_DS.setEmail("bruce-nguyen@hrboss.com");
		//DEFAULT_DS.setDBPassword("Hrb0ss2015EGMiISGPK9WZsl7DQ6zMJdTa");
		DEFAULT_DS.setType(DatasourceType.SALESFORCE.toString());
	}
	
	@Autowired
	CrmDatasourceManager crmMgr;
	
	@RequestMapping("/login/{email}/{password}")
    public String login(@PathVariable("email") String email,
    		@PathVariable("password") String password) {
    	
    	DataSource ds = new DataSource();
    	ds.setEmail(email);
    	ds.setDBPassword(password);
    	ds.setType(DatasourceType.SALESFORCE.toString());
    	try {
    		crmMgr.testConnection(ds);
    		return "Hello " + email + " !";
    	} catch (Exception e) {
    		return "Failed login into Salesforce account : " + email;
    	}
        
    }
    
	@RequestMapping("/describe")
    public String describeAll() {
    	try {
    		List<DataSet> metadata = crmMgr.buildObjectsMetadata(DEFAULT_DS, null);
    		return SalesforceObjectHelper.print(metadata);
    	} catch (Exception e) {
    		return e.getMessage();
    	}
    }
	
    @RequestMapping("/describe/{objectName}")
    public String describe(@PathVariable("objectName") String objectName) {
    	try {
    		List<DataSet> metadata = crmMgr.buildObjectsMetadata(DEFAULT_DS, objectName);
    		return SalesforceObjectHelper.print(metadata);
    	} catch (Exception e) {
    		return e.getMessage();
    	}
    }
    
    @RequestMapping("/measure/reports")
    public String scenario01() {
    	final DataSet template = new DataSet();
		final SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(DEFAULT_DS);
		ExecutorService reportExecutor = Executors.newFixedThreadPool(20);
		long duration = System.currentTimeMillis();
		List<DataSet> reportDS = new ArrayList<DataSet>();
    	try {
    		reportDS = ((CamelDatasourceManagerImpl)crmMgr).buildSalesforceReportsMetadata(template, creds, null, reportExecutor);
    	} catch (Exception e) {
    		return e.getMessage();
    	} finally {
    		reportExecutor.shutdown();
    	}
    	StringBuilder display = new StringBuilder();
    	display.append(String.format("Retrieving {%d} TABULAR reports takes {%d} m-seconds", reportDS.size(), System.currentTimeMillis() - duration));
    	display.append("<p>").append(SalesforceObjectHelper.print(reportDS)).append("</p>");
    	return display.toString();
    }
    
    @RequestMapping("/measure/objects")
    public String scenario02(@RequestParam(required = false, value = "withRowCount", defaultValue = "true") boolean withRowCount) {
    	final DataSet template = new DataSet();
		final SalesforceCredentials creds = SalesforceLoginConfigHelper.getCredentials(DEFAULT_DS);
		ExecutorService objectExecutor = Executors.newFixedThreadPool(100);
		long duration = System.currentTimeMillis();
		List<DataSet> objectDS = new ArrayList<DataSet>();
    	try {
    		objectDS = ((CamelDatasourceManagerImpl)crmMgr).buildSalesforceObjectsMetadata(template, creds, null, objectExecutor, withRowCount);
    	} catch (Exception e) {
    		return e.getMessage();
    	} finally {
    		objectExecutor.shutdown();
    	}
    	StringBuilder display = new StringBuilder(String.format("Retrieving {%d} objects", objectDS.size()));
    	if (withRowCount) {
    		display.append(" (with row counts) ");
    	} else {
    		display.append(" (no row counts) ");
    	}
    	display.append(String.format("takes {%d} m-seconds", System.currentTimeMillis() - duration));
    	display.append("<p>").append(SalesforceObjectHelper.print(objectDS)).append("</p>");
    	return display.toString();
    }
    
    @RequestMapping("/measure/objectsNreports")
    public String scenario03(@RequestParam(required = false, value = "withRowCount", defaultValue = "true") boolean withRowCount) {
		long duration = System.currentTimeMillis();
		List<DataSet> metadataList = new ArrayList<DataSet>();
    	try {
    		metadataList = crmMgr.buildObjectsMetadata(DEFAULT_DS, null);
    	} catch (Exception e) {
    		return e.getMessage();
    	}
    	StringBuilder display = new StringBuilder(String.format("Retrieving {%d} objects and reports", metadataList.size()));
    	if (withRowCount) {
    		display.append(" (with row counts) ");
    	} else {
    		display.append(" (no row counts) ");
    	}
    	display.append(String.format("takes {%d} m-seconds", System.currentTimeMillis() - duration));
    	display.append("<p>").append(SalesforceObjectHelper.print(metadataList)).append("</p>");
    	return display.toString();
    }
    
}
