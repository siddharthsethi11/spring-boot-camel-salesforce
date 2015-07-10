/**
 * Copyright (c) 2015 HRBoss
 */
package com.github.deeprot.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * @author Nguyen Nhat Quang
 *
 */
public enum DataType {

	
	
    NUMBER("Number"),
    TEXT("Text"),
    BOOLEAN("Boolean"),
    DATE("Date"),
    TIME("Time"),
    DATETIME("DateTime"),
    UNKNOWN("Unknown");

    private String name;
    
        private DataType(String name) {

        this.name = name;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public static DataType getDataType(String name) {

        for (DataType type : values()) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }
    
    /*
     * Types mapping to generic type
     * 
     * @author bruce.nguyen
     */
    private static final Map<DataType, List<String>> typeTables = new HashMap<DataType, List<String>>(){
    	{
    		put(TEXT, Arrays.asList("text", "string", "textarea", "url", "phone", "picklist"));
    		put(NUMBER, Arrays.asList("int", "double", "percent", "currency"));
    		put(BOOLEAN, Arrays.asList("boolean"));
    		put(DATE, Arrays.asList("date"));
    		put(DATETIME, Arrays.asList("datetime"));
    	}
    };
    
    /*
     * Guess the enum based on various type
     * 
     * @author bruce.nguyen
     */
    public static DataType guess(String type) {
    	Optional<Entry<DataType, List<String>>> dtCandidate = typeTables.entrySet().stream()
    			.filter(entry -> entry.getValue().contains(type)).findFirst();
    	if (dtCandidate.isPresent()) {
    		return dtCandidate.get().getKey();
    	} else {
    		return UNKNOWN;
    	}
    }
}
