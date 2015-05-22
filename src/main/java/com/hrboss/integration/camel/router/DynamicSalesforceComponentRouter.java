package com.hrboss.integration.camel.router;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_COMPONENT;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_COUNT_OBJECTS;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_FAILED_LOGIN;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_DESCRIPTION;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_GLOBAL_OBJECTS;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_OBJECT;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_OBJECT_WINDOW;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_VERSIONS;
import static com.hrboss.integration.camel.SalesforceProcessor.HEADER_CREDENTIALS;

import java.util.Map;

import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.camel.Consume;
import org.apache.camel.DynamicRouter;
import org.apache.camel.Header;
import org.apache.camel.Properties;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.hrboss.integration.camel.dto.QueryRecords;
import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.integration.helper.SalesforceLoginConfigHelper;

/**
 * Camel DynamicRouter for Salesforce Components, following the EIP pattern.
 * Each Salesforce component, managed within a Camel context, is bound to a user
 * credentials (email/password).
 * <p>
 * <b>NOTE:</b> while implementing Camel DynamicRouter, pay attention to the
 * exit condition (return NULL), otherwise the payload is routed forever within
 * the route itself.
 * </p>
 * For more information, visit <a
 * href="http://camel.apache.org/dynamic-router.html">here</a>, section
 * <b>Beware</b>
 * 
 * @author bruce.nguyen
 *
 */
@Component
public class DynamicSalesforceComponentRouter {

	private static final Logger LOG = LoggerFactory.getLogger(DynamicSalesforceComponentRouter.class);
	
	@Autowired
	protected CamelContext camelContext;
	
	/**
	 * Build the Salesforce Component name based on the user credentials. The
	 * name should be UNIQUE as the component will be cached.
	 * 
	 * @param creds
	 *            Salesforce Credentials
	 * @return the salesforce component name
	 */
	private String componentName(SalesforceCredentials creds) {
		return new StringBuilder("salesforce").append(creds.uniqueHash()).toString();
	}
	
	/**
	 * Ensure the Salesforce component of a user exists in the Camel context,
	 * and started. Otherwise the payload is not routed to the Salesforce API.
	 * 
	 * @param creds Salesforce Credentials
	 */
	private void ensureComponentExistence(SalesforceCredentials creds) {
		Assert.notNull(creds, "Salesforce Credentials must be set in the message header");
		SalesforceComponent component = (SalesforceComponent) camelContext.getComponent(componentName(creds));
		if (component == null) {
			component = new SalesforceComponent();
			component.setLoginConfig(SalesforceLoginConfigHelper.getLoginConfig(creds));
			camelContext.addComponent(componentName(creds), component);
		}
		try {
			if (!component.isStarted() && !component.isStarting()) {
				component.start();
			}
		} catch (Exception e) {
			LOG.error("Failed to start component {" + componentName(creds) + "} due to: " + e.getMessage(), e);
		}
	}
	
	/**
	 * Stop the component and evict it from the ProducerTemplate cache. This
	 * route should be normally called when user failed to log in or there is
	 * unexpected error with the user session which forces the user to relogin
	 * with proper credentials.
	 * 
	 * @param creds
	 *            Salesforce Credentials
	 */
	private void evictComponent(SalesforceCredentials creds) {
		Assert.notNull(creds, "Salesforce Credentials must be set in the message header");
		SalesforceComponent component = (SalesforceComponent) camelContext.getComponent(componentName(creds));
		try {
			if (component != null && !component.isStoppingOrStopped()) {
				component.stop(); // release the resource used by the component
			}
		} catch (Exception e) {
			LOG.error("Failed to stop component {" + componentName(creds) + "} due to: " + e.getMessage(), e);
		}
	}	

	/**
	 * Forward the payload from the DIRECT endpoint to the Salesforce component relevant to the credentials object.
	 * The exit condition ensures the payload to be forwarded ONCE.
	 * 
	 * @param creds SF credentials
	 * @param properties Exchange properties - this is modified after each forward loop 
	 * @return the next OUT endpoint
	 */
	private String slipOnce(SalesforceCredentials creds, Map<String, Object> properties, Object messageBody) {
		
		ensureComponentExistence(creds);

		if (properties.containsKey("CamelSlipEndpoint")) {
			/*
			 * NOTE: this is the exit condition
			 */
			return null;
		} else {
			String toEndpoint = properties.get("CamelToEndpoint").toString();
			if (toEndpoint.endsWith(FROM_URI_GET_VERSIONS)) {
				return componentName(creds) + ":getVersions";
			} else if (toEndpoint.endsWith(FROM_URI_GET_GLOBAL_OBJECTS)) {
				return componentName(creds) + ":getGlobalObjects";
			} else if (toEndpoint.endsWith(FROM_URI_GET_DESCRIPTION)) {
				return componentName(creds) + ":getDescription";
			} else if (toEndpoint.endsWith(FROM_URI_COUNT_OBJECTS)) {
				String objectName = (String) messageBody;
				return componentName(creds) + ":query?sObjectQuery=SELECT COUNT() FROM " + objectName + "&sObjectClass=" + QueryRecords.class.getName();
			} else if (toEndpoint.endsWith(FROM_URI_GET_OBJECT)) {
				return componentName(creds) + ":query?sObjectClass=" + QueryRecords.class.getName();
			} else if (toEndpoint.endsWith(FROM_URI_GET_OBJECT_WINDOW)) {
				return componentName(creds) + ":query?sObjectClass=" + QueryRecords.class.getName();
			} else {
				/*
				 * NOTE: this is the exit condition
				 */
				return null;
			}
		}
	}

	@Consume(uri = FROM_COMPONENT + FROM_URI_FAILED_LOGIN)
	@DynamicRouter
	public String failedLogin(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds) {
		evictComponent(creds);
		return null;
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_VERSIONS)
	@DynamicRouter
	public String getVersions(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, @Properties Map<String, Object> properties) {
		return slipOnce(creds, properties, null);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_GLOBAL_OBJECTS)
	@DynamicRouter
	public String getGlobalObjects(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, @Properties Map<String, Object> properties) {
		return slipOnce(creds, properties, null);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_DESCRIPTION)
	@DynamicRouter
	public String describeObject(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, @Properties Map<String, Object> properties) {
		return slipOnce(creds, properties, null);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_COUNT_OBJECTS)
	@DynamicRouter
	public String count(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, @Properties Map<String, Object> properties, @Body String objectName) {
		return slipOnce(creds, properties, objectName);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_OBJECT)
	@DynamicRouter
	public String get(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, 
			@Header("objectName") String objectName,
			@Header("fields") String fields,
			@Properties Map<String, Object> properties, @Body String query) {
		properties.put("objectName", objectName);
		properties.put("fields", fields);
		return slipOnce(creds, properties, query);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_OBJECT_WINDOW)
	@DynamicRouter
	public String getWindow(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, 
			@Header("objectName") String objectName,
			@Header("fields") String fields,
			@Properties Map<String, Object> properties, @Body String query) {
		properties.put("objectName", objectName);
		properties.put("fields", fields);
		return slipOnce(creds, properties, query);
	}
}
