package com.hrboss.integration.helper;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.Assert;

public class ObjectHelper {

	public static String debug(Object obj) throws Exception {
		Assert.notNull(obj);
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
	}
}
