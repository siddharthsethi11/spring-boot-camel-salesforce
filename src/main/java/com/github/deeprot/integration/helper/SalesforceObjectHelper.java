package com.github.deeprot.integration.helper;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.component.salesforce.api.SalesforceException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.deeprot.integration.camel.SalesforceProcessor.ReportType;
import com.github.deeprot.model.ColumnFieldMetadata;
import com.github.deeprot.model.DataType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mongodb.BasicDBObject;

/**
 * Helper class to operate on JSON-alike Salesforce data
 * 
 * @author bruce.nguyen
 *
 */
public class SalesforceObjectHelper {

	private static final Logger LOG = LoggerFactory.getLogger(SalesforceObjectHelper.class);
	
	private static final XmlMapper xmlMapper = new XmlMapper();
	static {
		xmlMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
		xmlMapper.setSerializationInclusion(Include.NON_NULL);
	}
	private static final ObjectMapper objectMapper = new ObjectMapper();
	static {
		objectMapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
		objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES,false);
		objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, false);
		objectMapper.setSerializationInclusion(Inclusion.NON_NULL);
	}
	private static Configuration jsonConfig = Configuration.defaultConfiguration();
	static {
		jsonConfig.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
	}

	/**
	 * Print object data as JSON string in pretty format
	 * 
	 * @param obj the target object
	 * @return JSON representation of the object
	 */
	public static String print(Object obj) {
		try {
			return objectMapper
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString(obj);
		} catch (Exception e) {
			return "Failed to print object: " + e.getMessage();
		}
	}

    /**
     * Convert an input stream to JSON structure
     *
     * @param inputStream the JSON-alike input stream
     * @return the JSON Map object
     */
    public static Map<String, ?> readJsonFromStream(InputStream inputStream) {
        try {
            return objectMapper.readValue(inputStream, new TypeReference<Map<String, ?>>(){});
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON from input stream", e);
            return null;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ex) {
                    LOG.error("Failed to close input stream.", ex);
                }
            }
        }
    }

	/**
	 * Convert Salesforce XML stream to Mongo basic object
	 * 
	 * @param inputStream the XML inputstream
	 * @param metadata the metadata map
	 * @return the collection of basic DB object
	 */
	public static Collection<BasicDBObject> readSalesforceXmlStreamToMongoObject(InputStream inputStream, Map<String, String> metadata) {
		try {
			List<Map<String, Object>> entries = xmlMapper.readValue(inputStream, List.class);
			return entries.parallelStream().map(mapObj -> fromMap(mapObj, metadata)).collect(Collectors.toList());
		} catch (Exception e) {
			LOG.warn("Failed to parse XML input stream", e);
		} finally {
			if (inputStream != null) try {
				inputStream.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * Convert Salesforce JSON report metadata to Mongo basic object
	 * 
	 * @param document the JSON document
	 * @param fieldList the composed field list object
	 * @return the number of records of that report - or 0 if the report is not TABULAR (to be filtered out)
	 * @throws Exception
	 */
	public static int readReportMetadata(DocumentContext document, BasicDBObject fieldList) throws Exception {
		String reportType = document.read("$.reportMetadata.reportFormat");
		List<String> reportAggregates = document.read("$['reportMetadata']['aggregates'][*]");
		int count = 0;
		switch (ReportType.valueOf(reportType)) {
			case TABULAR:
			case SUMMARY:
			case MATRIX:
				if (reportAggregates.contains("RowCount")) {
					count = document.read("$['factMap']['T!T']['aggregates'][(@.length-1)]['value']");
				} else {
					LOG.warn("The report {} doesn't support row count.", (String) document.read("$.reportMetadata.name"));
					count = -1;
				}
				Map<String, Map<String, Object>> columnInfos = document.read("$.reportExtendedMetadata.detailColumnInfo");
				columnInfos.entrySet().stream().forEach(entry -> {
					StringBuilder key = normalizeFieldname(entry.getKey());
					ColumnFieldMetadata columnMetadata = new ColumnFieldMetadata();
					columnMetadata.setAlias((String) entry.getValue().get("label"));
					columnMetadata.setType(DataType.guess((String) entry.getValue().get("dataType")).getName());
					fieldList.put(key.toString(), columnMetadata);
				});
				break;
			case JOINED:	
				LOG.error("The current report's format {} will be supported in the next release.", reportType);
		}
		return count;
	}

	/**
	 * Convert Salesforce JSON report to Mongo basic object
	 * 
	 * @param inputStream the JSON inputstream
	 * @return the collection of basic DB object
	 */
	public static Collection<BasicDBObject> readSalesforceReportDataStreamToMongoObject(InputStream inputStream) {
		DocumentContext document;
		try {
			Map<String, ?> jsonData = readJsonFromStream(inputStream);
			document = JsonPath.using(jsonConfig).parse(jsonData);
			String reportType = document.read("$.reportMetadata.reportFormat");
			switch (ReportType.valueOf(reportType)) {
				case TABULAR:
				case SUMMARY:
				case MATRIX:
					return readReportData(document);
				case JOINED:	
					LOG.error("The current report's format {} will be supported in the next release.", reportType);
					break;
			}
		} catch (Exception e) {
			LOG.warn("Failed to parse JSON input stream", e);
		}
		return null;
	}
	
	/**
	 * Parse a JSON-alike report data having format TABULAR to MongoDB object using jSonPath
	 * 
	 * @param document Json document
	 * @return MongoDB object
	 * @throws Exception
	 */
	private static Collection<BasicDBObject> readReportData(DocumentContext document) throws Exception {
		Map<String, Map<String, Object>> columnInfos = document.read("$.reportExtendedMetadata.detailColumnInfo");
		final Map<String, String> metadata = columnInfos.entrySet().parallelStream().
				collect(Collectors.<Map.Entry<String, Map<String, Object>>, String, String>toMap(Map.Entry::getKey, p -> (String) p.getValue().get("dataType")));
		List<List<Map<String, Object>>> jsonRows = document.read("$['factMap'][*]['rows'][*]['dataCells']");
		List<String> columnList = document.read("$.reportMetadata.detailColumns");
		return jsonRows.parallelStream().map(row -> {
			Map<String, Map<String, Object>> rowObj = new HashMap<>();
			Iterator<String> keyIterator = columnList.iterator();
			Iterator<Map<String, Object>> valueIterator = row.stream().iterator();
			while (keyIterator.hasNext() && valueIterator.hasNext()) {
				rowObj.put(keyIterator.next(), valueIterator.next());
			}
			return fromMap(rowObj, metadata);
		}).collect(Collectors.toList());
	}
	
	/**
	 * Convert Salesforce attributes map to Mongo basic object
	 * 
	 * @param mapObj
	 * @param metadata
	 * @return
	 */
	private static BasicDBObject fromMap(Map<String, ?> mapObj, Map<String, String> metadata) {
		final BasicDBObject mObject = new BasicDBObject();
		mapObj.entrySet().forEach(entry -> {
			StringBuilder normalizedKey = normalizeFieldname(entry.getKey());
			try {
				String type = metadata.get(entry.getKey());
				if (entry.getValue() instanceof LinkedHashMap) {
					LinkedHashMap<String, String> fieldData = (LinkedHashMap<String, String>) entry.getValue();
					if (fieldData.containsKey("nil")) {
						mObject.put(normalizedKey.toString(), null);
					} else if (fieldData.containsKey("label") && fieldData.containsKey("value")){
						if ("string".equals(type)) {
							mObject.put(normalizedKey.toString(), fieldData.get("label"));
						} else {
							mObject.put(normalizedKey.toString(), fieldData.get("value"));
						}
					} else {
						mObject.put(normalizedKey.toString(), fieldData);
					}
				} else if (entry.getValue() instanceof String) {
					
					String value = (String) entry.getValue();
					if ("string".equals(type) || "textarea".equals(type)) {
						mObject.put(normalizedKey.toString(), value);
					} else if ("int".equals(type)) {
						mObject.put(normalizedKey.toString(), Integer.valueOf(value));
					} else if ("double".equals(type) || "percent".equals(type) || "currency".equals(type)) {
						mObject.put(normalizedKey.toString(), Double.valueOf(value));
					} else if ("boolean".equals(type)) {
						mObject.put(normalizedKey.toString(), Boolean.valueOf(value));
					} else if ("date".equals(type) || "datetime".equals(type)) {
						mObject.put(normalizedKey.toString(), parseDate(value));
					} else {
						mObject.put(normalizedKey.toString(), entry.getValue());
					}
				} else {
					mObject.put(normalizedKey.toString(), entry.getValue());
				}
			} catch (Exception e) {
				LOG.warn(String.format("Failed parsing field {%s} with value {%s}", normalizedKey.toString(), entry.getValue()), e);
				mObject.put(normalizedKey.toString(), null);
			}
		});
		return mObject;
	}

	// Column name in MongoDB should NOT contain "." or "$"
	// Refer to: http://docs.mongodb.org/manual/faq/developers/#faq-dollar-sign-escaping
	public static StringBuilder normalizeFieldname(String fieldname) {
		return new StringBuilder(fieldname.replaceAll("\\.", "!"));
	}
	
	/**
	 * Parse a String value in Salesforce to a Date object.
	 * 
	 * @param value
	 * @return
	 */
	private static Date parseDate(String value) {
		Date aDate = null;
		try {
			aDate = Date.from(LocalDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME).atZone(ZoneId.systemDefault()).toInstant());
		} catch (DateTimeParseException dtpe) {
			try {
				aDate = Date.from(LocalDateTime.parse(value+"T00:00:00", DateTimeFormatter.ISO_DATE_TIME).atZone(ZoneId.systemDefault()).toInstant());
			} catch (DateTimeParseException ex) {
				LOG.warn("Failed to parse date string {}", value, ex);
			}
		} catch (NullPointerException npe) {
			LOG.warn("Failed to parse null date string", npe);
		}
		return aDate;
	}

	/**
	 * Throw the root cause in the exception stack
	 *
	 * @param e
	 * @throws Exception
	 */
	public static void throwRootCause(Exception e) throws Exception {
		if (e instanceof CamelExecutionException) {
			Throwable t = e.getCause();
			while (t.getCause() != null) {
				t = t.getCause();
			}
			//Caused by: org.apache.camel.component.salesforce.api.SalesforceException: {errors:[{"errorCode":"REQUEST_LIMIT_EXCEEDED","message":"TotalRequests Limit exceeded."}],statusCode:403}
			if (t instanceof SalesforceException) {
				DocumentContext document = JsonPath.using(jsonConfig).parse(t.getMessage());
				List<String> errorCodes = document.read("$.errors[?(@.errorCode)]");
				if (errorCodes.contains("REQUEST_LIMIT_EXCEEDED") || errorCodes.contains("FORBIDDEN")) {
					throw new Exception("ERROR_SALESFORCE_API_LIMIT_EXCEED", t);
				}
			}
		}
		throw new Exception("ERROR_SALESFORCE_UNKNOWN_ERROR", e);

	}
}
