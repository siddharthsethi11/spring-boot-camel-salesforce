package com.hrboss.integration.helper;

import java.io.File;
import java.io.FileInputStream;
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
import com.hrboss.integration.camel.SalesforceProcessor.ReportType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mongodb.BasicDBObject;


public class SalesforceObjectHelper {

	private static final Logger LOG = LoggerFactory.getLogger(SalesforceObjectHelper.class);
	
	private static final XmlMapper xmlMapper = new XmlMapper();
	{
		xmlMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
		xmlMapper.setSerializationInclusion(Include.NON_NULL);
	}
	private static final ObjectMapper objectMapper = new ObjectMapper();
	{
		objectMapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
		objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES,false);
		objectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, false);
		objectMapper.setSerializationInclusion(Inclusion.NON_NULL);
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
	 * Convert XML stream to a collection of Map objects
	 * 
	 * @param inputStream the XML inputstream
	 * @return a collection of Map objects
	 */
	public static Collection<Map<String, Object>> readXmlStreamCollection(InputStream inputStream) {
		try {
			List<Map<String, Object>> entries = xmlMapper.readValue(inputStream, List.class);
			return entries.stream().map(map -> map.entrySet().parallelStream()
					.map(entry -> {
						if (entry.getValue() instanceof LinkedHashMap 
								&& ((LinkedHashMap)entry.getValue()).containsKey("nil")) {
							entry.setValue("");
						}
						return entry;
					})
					.collect(Collectors.<Map.Entry<String, Object>, String, Object>toMap(Map.Entry::getKey, Map.Entry::getValue))
			).collect(Collectors.toList());
		} catch (Exception e) {
			LOG.warn("Failed to parse XML input stream", e);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return null;
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
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return null;
	}
	
	/**
	 * Convert Salesforce JSON report to Mongo basic object
	 * 
	 * @param inputStream the JSON inputstream
	 * @param metadata the metadata map
	 * @return the collection of basic DB object
	 */
	public static Collection<BasicDBObject> readSalesforceReportDataStreamToMongoObject(InputStream inputStream) {
		Configuration conf = Configuration.defaultConfiguration();
		conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		DocumentContext document = null;
		try {
			Map<String, Object> jsonData = objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>(){});
			document = JsonPath.using(conf).parse(jsonData);
			String reportType = document.read("$.reportMetadata.reportFormat");
			switch (ReportType.valueOf(reportType)) {
				case TABULAR:
					return readTabularReportData(document);
				case SUMMARY:
				case JOINED:
				case MATRIX:
					LOG.error("The current report's format {} will be supported in the next release.", reportType);
					break;
			}
		} catch (Exception e) {
			LOG.warn("Failed to parse JSON input stream", e);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (Exception ex) {
					LOG.error("Failed to close input stream.", ex);
				}
			}
		}
		return null;
	}
	
	private static Collection<BasicDBObject> readTabularReportData(DocumentContext document) throws Exception {
		Map<String, Map<String, Object>> columnInfos = document.read("$.reportExtendedMetadata.detailColumnInfo");
		final Map<String, String> metadata = columnInfos.entrySet().parallelStream().
				collect(Collectors.<Map.Entry<String, Map<String, Object>>, String, String>toMap(Map.Entry::getKey, p -> (String) p.getValue().get("dataType")));
		List<List<Map<String, Object>>> jsonRows = document.read("$['factMap']['T!T']['rows'][*]['dataCells']");
		List<String> columnList = document.read("$.reportMetadata.detailColumns");
		return jsonRows.parallelStream().map(row -> {
			Map<String, Object> rowObj = new HashMap<String, Object>(); 
			Iterator<String> keyIterator = columnList.iterator();
			Iterator<Object> valueIterator = row.stream().map(map -> map.get("value")).iterator();
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
	private static BasicDBObject fromMap(Map<String, Object> mapObj, Map<String, String> metadata) {
		final BasicDBObject mObject = new BasicDBObject();
		mapObj.entrySet().forEach(entry -> {
			try {
				if (entry.getValue() instanceof LinkedHashMap) {
					LinkedHashMap<String, String> fieldData = (LinkedHashMap<String, String>) entry.getValue();
					if (fieldData.containsKey("nil")) {
						mObject.put(entry.getKey(), null);
					} else {
						mObject.put(entry.getKey(), fieldData);
					}
				} else if (entry.getValue() instanceof String) {
					String type = metadata.get(entry.getKey());
					String value = (String) entry.getValue();
					if ("string".equals(type) || "textarea".equals(type)) {
						mObject.put(entry.getKey(), value);
					} else if ("int".equals(type)) {
						mObject.put(entry.getKey(), Integer.valueOf(value));
					} else if ("double".equals(type) || "percent".equals(type) || "currency".equals(type)) {
						mObject.put(entry.getKey(), Double.valueOf(value));
					} else if ("boolean".equals(type)) {
						mObject.put(entry.getKey(), Boolean.valueOf(value));
					} else if ("date".equals(type) || "datetime".equals(type)) {
						mObject.put(entry.getKey(), parseDate(value));
					} else {
						mObject.put(entry.getKey(), print(entry.getValue()));
					}
				} else {
					mObject.put(entry.getKey(), entry.getValue());
				}
			} catch (Exception e) {
				LOG.warn("Failed parsing field {} with value {}", entry.getKey(), entry.getValue(), e);
				mObject.put(entry.getKey(), null);
			}
		});
		return mObject;
	}
	
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
	
	public static void main(String[] args) throws Exception {
		File aFile = new File("D:\\data\\salesforce\\all-accounts-bulkapidata.xml");
		File metadataFile = new File("D:\\data\\salesforce\\account.json");
		Map<String, Object> md = objectMapper.readValue(new FileInputStream(metadataFile), new TypeReference<HashMap<String, Object>>(){});
		Map<String, String> metadata = ((List<LinkedHashMap>)md.get("fields")).stream()
				.collect(Collectors.<LinkedHashMap<String, String>, String, String>toMap(x -> x.get("name"), x -> x.get("type")));
		Collection<BasicDBObject> records = SalesforceObjectHelper.readSalesforceXmlStreamToMongoObject(new FileInputStream(aFile), metadata);
		records.stream().forEach(obj -> System.out.println(print(obj)));
		
		/*File metadataFile = new File("D:\\data\\salesforce\\describe-report.json");
		Map<String, Object> reportMetadata = objectMapper.readValue(new FileInputStream(metadataFile), new TypeReference<Map<String,Object>>(){});
		Configuration conf = Configuration.defaultConfiguration();
		conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		DocumentContext document = JsonPath.using(conf).parse(reportMetadata);
		System.out.println("Total records: " + document.read("$['factMap']['T!T']['aggregates'][0]['value']"));
		Map<String, Map<String, Object>> columnInfos = document.read("$.reportExtendedMetadata.detailColumnInfo");
		columnInfos.entrySet().stream().forEach(entry -> {
			System.out.println(String.format("Inspecting field %s: label {%s}, type {%s}", 
					entry.getKey(),
					(String) entry.getValue().get("label"),
					(String) entry.getValue().get("dataType")));
		});*/
		
		/*
		File dataFile = new File("D:\\data\\salesforce\\all-contacts-list-reportdata.json");
		Collection<BasicDBObject> reportData = readSalesforceReportDataStreamToMongoObject(new FileInputStream(dataFile));
		reportData.forEach(row -> System.out.println(print(row)));
		*/
	}
}
