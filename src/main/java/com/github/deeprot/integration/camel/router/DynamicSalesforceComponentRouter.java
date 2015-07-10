package com.github.deeprot.integration.camel.router;
import static com.github.deeprot.integration.camel.SalesforceProcessor.CONNECTION_TIMEOUT;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_COMPONENT;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_CHECK_BATCH_STATUS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_CLOSE_JOB;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_COUNT_OBJECTS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_CREATE_BATCH;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_CREATE_JOB;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_CREATE_REPORT_INSTANCE;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_FAILED_LOGIN;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_BATCH_DATA;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_BATCH_RESULTS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_DESCRIPTION;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_GLOBAL_OBJECTS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_INSTANCE_DATA;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_OBJECT;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_OBJECT_WINDOW;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_GET_VERSIONS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.FROM_URI_LIST_REPORTS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.HEADER_CREDENTIALS;
import static com.github.deeprot.integration.camel.SalesforceProcessor.RESPONSE_TIMEOUT;

import java.util.Map;

import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.camel.Consume;
import org.apache.camel.DynamicRouter;
import org.apache.camel.Header;
import org.apache.camel.Properties;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;
import org.apache.camel.component.salesforce.internal.OperationName;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.github.deeprot.integration.camel.dto.QueryRecords;
import com.github.deeprot.integration.camel.dto.SalesforceCredentials;
import com.github.deeprot.integration.helper.SalesforceLoginConfigHelper;

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
	private synchronized SalesforceComponent ensureComponentExistence(SalesforceCredentials creds) {
		Assert.notNull(creds, "Salesforce Credentials must be set in the message header");
		SalesforceComponent component = (SalesforceComponent) camelContext.getComponent(componentName(creds));
		if (component == null) {
			component = new SalesforceComponent();
			// configure login
			component.setLoginConfig(SalesforceLoginConfigHelper.getLoginConfig(creds));
			// configure HttpClient
			SalesforceEndpointConfig config = new SalesforceEndpointConfig();
			HttpClient httpClient = new HttpClient();
            httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
            httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
            httpClient.setTimeout(RESPONSE_TIMEOUT);
            config.setHttpClient(httpClient);
            component.setConfig(config);
            
			camelContext.addComponent(componentName(creds), component);
		}
		try {
			if (!component.isStarted() && !component.isStarting()) {
				component.start();
			}
			LOG.debug("Salesforce component session: " + component.getSession().getAccessToken());
		} catch (Exception e) {
			LOG.error("Failed to start component {" + componentName(creds) + "} due to: " + e.getMessage(), e);
		}
		return component;
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
			} else if (toEndpoint.endsWith(FROM_URI_CREATE_JOB)) {
				return componentName(creds) + ":createJob";
			} else if (toEndpoint.endsWith(FROM_URI_CLOSE_JOB)) {
				return componentName(creds) + ":closeJob";
			} else if (toEndpoint.endsWith(FROM_URI_CREATE_BATCH)) {
				return componentName(creds) + ":createBatch?contentType="
						+ properties.get("contentType") + "&jobId="
						+ properties.get("jobId");
			} else if (toEndpoint.endsWith(FROM_URI_CHECK_BATCH_STATUS)) {
				return componentName(creds) + ":getBatch";
			} else if (toEndpoint.endsWith(FROM_URI_GET_BATCH_RESULTS)) {
				return componentName(creds) + ":getQueryResultIds" + "?jobId="
				+ properties.get("jobId");
			} else if (toEndpoint.endsWith(FROM_URI_GET_BATCH_DATA)) {
				return componentName(creds) + ":getQueryResult?"
						+ "jobId=" + properties.get("jobId")
						+ "&batchId=" + properties.get("batchId");
			} else if (toEndpoint.endsWith(FROM_URI_LIST_REPORTS)) {
				StringBuilder query = new StringBuilder(componentName(creds))
					.append(":query?sObjectClass=").append(QueryRecords.class.getName());
				return query.toString();
			} else if (toEndpoint.endsWith(FROM_URI_CREATE_REPORT_INSTANCE)) {
				return componentName(creds) + ":" 
					+ OperationName.CREATE_REPORT_DATA_INSTANCE.value()
					+ "?includeDetails=" + properties.get("includeDetails");
			} else if (toEndpoint.endsWith(FROM_URI_GET_INSTANCE_DATA)) {
				return componentName(creds) + ":" 
					+ OperationName.GET_REPORT_INSTANCE_DATA.value()
					+ "?reportId=" + properties.get("reportId");
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
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_CREATE_JOB)
	@DynamicRouter
	public String bulkCreateJob(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, 
			@Properties Map<String, Object> properties) {
		return slipOnce(creds, properties, null);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_CLOSE_JOB)
	@DynamicRouter
	public String bulkCloseJob(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds, 
			@Properties Map<String, Object> properties) {
		return slipOnce(creds, properties, null);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_CREATE_BATCH)
	@DynamicRouter
	public String bulkCreateBatch(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Header("jobId") String jobId,
			@Header("contentType") String contentType,
			@Properties Map<String, Object> properties, @Body String query) {
		properties.put("jobId", jobId);
		properties.put("contentType", contentType);
		return slipOnce(creds, properties, query);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_CHECK_BATCH_STATUS)
	@DynamicRouter
	public String bulkGetBatch(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Properties Map<String, Object> properties, @Body BatchInfo batchInfo) {
		return slipOnce(creds, properties, batchInfo);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_BATCH_RESULTS)
	@DynamicRouter
	public String bulkGetBatchResults(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Header("jobId") String jobId,
			@Properties Map<String, Object> properties, @Body String batchId) {
		properties.put("jobId", jobId);	
		return slipOnce(creds, properties, batchId);
	}
	
	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_BATCH_DATA)
	@DynamicRouter
	public String bulkGetBatchResultData(@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Header("jobId") String jobId,
			@Header("batchId") String batchId,
			@Properties Map<String, Object> properties, @Body Object body) {

		if (body instanceof String) {
			properties.put("jobId", jobId);
			properties.put("batchId", batchId);
			return slipOnce(creds, properties, body);
		} else {
			return null;
		}
	}	

	@Consume(uri = FROM_COMPONENT + FROM_URI_LIST_REPORTS)
	@DynamicRouter
	public String listReports(
			@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Properties Map<String, Object> properties, @Body String query) {
		return slipOnce(creds, properties, query);
	}

	@Consume(uri = FROM_COMPONENT + FROM_URI_CREATE_REPORT_INSTANCE)
	@DynamicRouter
	public String describeReport(
			@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Header("includeDetails") boolean includeDetails,
			@Properties Map<String, Object> properties, @Body String reportId) {
		properties.put("includeDetails", includeDetails);
		return slipOnce(creds, properties, reportId);
	}

	@Consume(uri = FROM_COMPONENT + FROM_URI_GET_INSTANCE_DATA)
	@DynamicRouter
	public String getReportData(
			@Header(HEADER_CREDENTIALS) SalesforceCredentials creds,
			@Header("reportId") String reportId,
			@Properties Map<String, Object> properties, @Body Object body) {
		if (body instanceof String) {
			properties.put("reportId", reportId);
			return slipOnce(creds, properties, body);
		} else {
			return null;
		}
	}
}
