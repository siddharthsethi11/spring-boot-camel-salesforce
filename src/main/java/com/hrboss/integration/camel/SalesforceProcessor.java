package com.hrboss.integration.camel;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.api.dto.GlobalObjects;
import org.apache.camel.component.salesforce.api.dto.SObject;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.Version;
import org.apache.camel.component.salesforce.api.dto.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.hrboss.integration.camel.dto.QueryRecords;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.camel.router.DynamicSalesforceComponentRouter;
import com.hrboss.integration.helper.ObjectHelper;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;

@Component
public class SalesforceProcessor extends RouteBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(SalesforceProcessor.class);

	//public static final String HEADER_CREDENTIALS = "credentials";
	public static final String COMPONENT_NAME = "SalesforceComponent";
	public static final String FROM_COMPONENT = "direct:";
	//public static final String FROM_URI_FAILED_LOGIN = "failedLogin";
	public static final String FROM_URI_GET_VERSIONS = "getVersions";
	public static final String FROM_URI_GET_GLOBAL_OBJECTS = "getGlobalObjects";
	public static final String FROM_URI_GET_DESCRIPTION = "describeObject";
	public static final String FROM_URI_GET_OBJECT = "getObject";
	public static final String FROM_URI_COUNT_OBJECTS = "countObject";
	public static final String FROM_URI_IMPORT_OBJECT = "import";
	
	@Autowired
	ProducerTemplate template;
	
	private String componentName(SalesforceCredentials creds) {
		return new StringBuilder("salesforce").append(creds.uniqueHash()).toString();
	}
	
	private void ensureComponentExistence(SalesforceCredentials creds) {
		Assert.notNull(creds, "Salesforce Credentials must be set in the message header");
		SalesforceComponent component = (SalesforceComponent) template.getCamelContext().getComponent(componentName(creds));
		if (component == null) {
			component = new SalesforceComponent();
			component.setLoginConfig(SalesforceLoginConfigHelper.getLoginConfig(creds));
			//component.setPackages("org.apache.camel.salesforce.dto");
			template.getCamelContext().addComponent(componentName(creds), component);
		}
		try {
			if (!component.isStarted() && !component.isStarting()) {
				component.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void evictComponent(SalesforceCredentials creds) {
		Assert.notNull(creds, "Salesforce Credentials must be set in the message header");
		SalesforceComponent component = (SalesforceComponent) template.getCamelContext().getComponent(componentName(creds));
		try {
			if (component != null && !component.isStoppingOrStopped()) {
				component.stop(); // release the resource used by the component
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public Collection<Version> getVersions(SalesforceCredentials creds) throws Exception {
		List<Version> versions = null;
		ensureComponentExistence(creds);
		try {
			Object o = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_VERSIONS, (Object) null, COMPONENT_NAME, componentName(creds));
			if (o instanceof Versions) {
				versions = ((Versions) o).getVersions();
			} else {
				versions = (List<Version>) o;
			}
			debug(versions);
		} catch (CamelExecutionException camelEx) {
			evictComponent(creds);
			throw camelEx;
		}
		return versions;
	}

	public Collection<SObject> getObjectTypes(SalesforceCredentials creds, String objectName) throws Exception {
		ensureComponentExistence(creds);
		GlobalObjects globalObjects = (GlobalObjects) template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_GLOBAL_OBJECTS, (Object) null, COMPONENT_NAME, componentName(creds));
		debug(globalObjects);
		if (!StringUtils.isEmpty(objectName)) {
			for (SObject sObj : globalObjects.getSobjects()) {
				if (objectName.equals(sObj.getName())) {
					return Arrays.asList(sObj);
				}
			}
			//return globalObjects.getSobjects().stream()
			//		.filter(sObj -> objectName.equals(sObj.getName()))
			//		.collect(Collectors.toList());
		}
		return globalObjects.getSobjects();
	}
	
	public SObjectDescription describeObject(SalesforceCredentials creds, final String objectName) throws Exception {
		Assert.notNull(objectName, "objectName should not be null");
		ensureComponentExistence(creds);
		SObjectDescription objectDescription = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_GET_DESCRIPTION, objectName, COMPONENT_NAME, componentName(creds), SObjectDescription.class);
		debug(objectDescription);
		return objectDescription;
	}
	
	public int countObject(SalesforceCredentials creds, final SObject sObj) throws Exception {
		ensureComponentExistence(creds);
		Assert.notNull(sObj, "SObject should not be null");
		QueryRecords<?> queryResult = template.requestBodyAndHeader(FROM_COMPONENT + FROM_URI_COUNT_OBJECTS, sObj.getName(), COMPONENT_NAME, componentName(creds), QueryRecords.class);
		debug(queryResult);
		Assert.notNull(queryResult, "QueryRecords should not be null");
		return queryResult.getTotalSize();
	}
	
	public Object getObject(final SalesforceCredentials creds, final List<String> fields, final String objectName, final String objectId) throws Exception {
		//Assert.notNull(sObj, "SObject should not be null");
		ensureComponentExistence(creds);
		Map<String, Object> headers = new HashMap<String, Object>() {
			{
			put(COMPONENT_NAME, componentName(creds));
			put("objectName", objectName);
			put("fields", StringUtils.collectionToCommaDelimitedString(fields));
			}
		};
		Object o = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_GET_OBJECT, objectId, headers);
		debug(o);
		return o;
	}
	
	/*public List<?> fetchObjects(final SalesforceCredentials creds, final String objectName, final int limit, final int offset) throws Exception {
		Assert.notNull(objectName, "Object Name should not be null");
		ensureComponentExistence(creds);
		Map<String, Object> headers = new HashMap<String, Object>() {
			{
			put(HEADER_CREDENTIALS, creds);
			put("limit", limit);
			put("offset", offset);
			}
		};
		
		List<?> queryResult = template.requestBodyAndHeaders(FROM_COMPONENT + FROM_URI_IMPORT_OBJECT, objectName, headers, List.class);
		debug(queryResult);
		Assert.notNull(queryResult, "Result should not be null");
		return queryResult;
	}*/
	
	private void debug(Object obj) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(ObjectHelper.debug(obj));
		}
	}
	
	@Override
	public void configure() throws Exception {
		from(FROM_COMPONENT + FROM_URI_GET_VERSIONS).dynamicRouter(method(DynamicSalesforceComponentRouter.class, "getVersion"));
	}

}
