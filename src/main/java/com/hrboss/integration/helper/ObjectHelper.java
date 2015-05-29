package com.hrboss.integration.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;


public class ObjectHelper {

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
	
	public static String print(Object obj) {
		try {
			return objectMapper
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString(obj);
		} catch (Exception e) {
			return "Failed to print object: " + e.getMessage();
		}
	}
	
	public static Collection<Map<String, Object>> readXmlStreamToJson(InputStream inputStream) {
		try {
			List<Map<String, Object>> entries = xmlMapper.readValue(inputStream, List.class);
			return entries.stream().map(map -> map.entrySet().stream()
					.map(entry -> {
						if (entry.getValue() instanceof LinkedHashMap && ((LinkedHashMap)entry.getValue()).containsKey("nil")) {
							entry.setValue("");
						}
						return entry;
					})
					.collect(Collectors.<Map.Entry<String, Object>, String, Object>toMap(Map.Entry::getKey, Map.Entry::getValue))
			).collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
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
	
	public static void main(String[] args) throws Exception {
		File aFile = new File("D:\\75210000000Y20k.xml");
		Collection<?> records = ObjectHelper.readXmlStreamToJson(new FileInputStream(aFile));
		if (records != null && records.size() > 0) {
			records.stream().forEach(obj -> System.out.println(ObjectHelper.print(obj)));
		}
	}
}
