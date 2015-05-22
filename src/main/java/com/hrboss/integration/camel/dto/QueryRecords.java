package com.hrboss.integration.camel.dto;

import java.util.List;

import org.apache.camel.component.salesforce.api.dto.AbstractQueryRecordsBase;

import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * Wrapper objects for SOQL query
 * 
 * @author bruce.nguyen
 *
 * @param <T>
 */
public class QueryRecords<T> extends AbstractQueryRecordsBase {

	@XStreamImplicit
    private List<T> records;
	
    public List<T> getRecords() {
        return records;
    }

    public void setRecords(List<T> records) {
        this.records = records;
    }
}
