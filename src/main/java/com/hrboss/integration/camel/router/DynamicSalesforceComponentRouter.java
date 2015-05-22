package com.hrboss.integration.camel.router;

import static com.hrboss.integration.camel.SalesforceProcessor.COMPONENT_NAME;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_COUNT_OBJECTS;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_DESCRIPTION;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_GLOBAL_OBJECTS;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_OBJECT;
import static com.hrboss.integration.camel.SalesforceProcessor.FROM_URI_GET_VERSIONS;

import java.util.Map;

import org.apache.camel.Body;
import org.apache.camel.Header;
import org.apache.camel.Properties;
import org.springframework.stereotype.Component;

import com.hrboss.integration.camel.dto.QueryRecords;

@Component
public class DynamicSalesforceComponentRouter {

	public String route(Map<String, Object> properties, Object messageBody) {
		
		if (properties.containsKey("CamelSlipEndpoint")) {
			return null;
		} else {
			
			String toEndpoint = properties.get("CamelToEndpoint").toString();
			String componentName = properties.get(COMPONENT_NAME).toString();
			
			if (toEndpoint.endsWith(FROM_URI_GET_VERSIONS)) {
				
				return componentName + ":getVersions";
			} else if (toEndpoint.endsWith(FROM_URI_GET_GLOBAL_OBJECTS)) {
				
				return componentName + ":getGlobalObjects";
			} else if (toEndpoint.endsWith(FROM_URI_GET_DESCRIPTION)) {
				
				return componentName + ":getDescription";
			} else if (toEndpoint.endsWith(FROM_URI_COUNT_OBJECTS)) {
				
				String objectName = (String) messageBody;
				return componentName + ":query?sObjectQuery=SELECT id FROM " + objectName + "&sObjectClass=" + QueryRecords.class.getName();
			} else if (toEndpoint.endsWith(FROM_URI_GET_OBJECT)) {

				String objectName = properties.get("objectName").toString();
				String fields = properties.get("fields").toString();
				String id = (String) messageBody;
				return componentName + ":query?sObjectQuery=SELECT " + fields + " FROM " + objectName + " WHERE id = " + id + "&sObjectClass=" + QueryRecords.class.getName();
			} else {
				return null;
			}
		}
	}
	
	//@Consume(uri = FROM_COMPONENT + FROM_URI_GET_VERSIONS)
	//@DynamicRouter
	public String getVersion(@Header(COMPONENT_NAME) String componentName, @Properties Map<String, Object> properties, @Body Object messageBody) {
		properties.put(COMPONENT_NAME, componentName);
		return route(properties, messageBody);
	}
	
	//@Consume(uri = FROM_COMPONENT + FROM_URI_GET_GLOBAL_OBJECTS)
	//@DynamicRouter
	public String getGlobalObjects(@Header(COMPONENT_NAME) String componentName, @Properties Map<String, Object> properties, @Body Object messageBody) {
		properties.put(COMPONENT_NAME, componentName);
		return route(properties, messageBody);
	}
	
	//@Consume(uri = FROM_COMPONENT + FROM_URI_GET_DESCRIPTION)
	//@DynamicRouter
	public String getDescription(@Header(COMPONENT_NAME) String componentName, @Properties Map<String, Object> properties, @Body Object messageBody) {
		properties.put(COMPONENT_NAME, componentName);
		return route(properties, messageBody);
	}
	
	//@Consume(uri = FROM_COMPONENT + FROM_URI_COUNT_OBJECTS)
	//@DynamicRouter
	public String countObjects(@Header(COMPONENT_NAME) String componentName, @Properties Map<String, Object> properties, @Body Object messageBody) {
		properties.put(COMPONENT_NAME, componentName);
		return route(properties, messageBody);
	}
	
	//@Consume(uri = FROM_COMPONENT + FROM_URI_GET_OBJECT)
	//@DynamicRouter
	public String getObject(@Header(COMPONENT_NAME) String componentName, @Header("fields") String fields, @Header("objectName") String objectName, 
			@Properties Map<String, Object> properties, @Body Object messageBody) {
		properties.put(COMPONENT_NAME, componentName);
		properties.put("objectName", objectName);
		properties.put("fields", fields);
		return route(properties, messageBody);
	}

}
