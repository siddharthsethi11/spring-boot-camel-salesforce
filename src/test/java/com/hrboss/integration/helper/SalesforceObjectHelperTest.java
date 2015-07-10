package com.hrboss.integration.helper;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.deeprot.integration.helper.SalesforceObjectHelper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mongodb.BasicDBObject;

public class SalesforceObjectHelperTest {

	//@Test
	public void testReadSalesforceXmlStreamToMongoObject() throws Exception {
		File xml = new File("src/test/resources/75228000000PDuW.xml");
		Iterator<String> fieldnames = Arrays.asList(new String[] { "Id",
				"IsDeleted", "MasterRecordId", "Name", "Type", "ParentId",
				"BillingStreet", "BillingCity", "BillingState",
				"BillingPostalCode", "BillingCountry", "BillingLatitude",
				"BillingLongitude", "BillingAddress", "ShippingStreet",
				"ShippingCity", "ShippingState", "ShippingPostalCode",
				"ShippingCountry", "ShippingLatitude", "ShippingLongitude",
				"ShippingAddress", "Phone", "Fax", "AccountNumber", "Website",
				"PhotoUrl", "Sic", "Industry", "AnnualRevenue",
				"NumberOfEmployees", "Ownership", "TickerSymbol",
				"Description", "Rating", "Site", "OwnerId", "CreatedDate",
				"CreatedById", "LastModifiedDate", "LastModifiedById",
				"SystemModstamp", "LastActivityDate", "LastViewedDate",
				"LastReferencedDate", "Jigsaw", "JigsawCompanyId",
				"CleanStatus", "AccountSource", "DunsNumber", "Tradestyle",
				"NaicsCode", "NaicsDesc", "YearStarted", "SicDesc",
				"DandbCompanyId", "CustomerPriority__c", "SLA__c", "Active__c",
				"NumberofLocations__c", "UpsellOpportunity__c",
				"SLASerialNumber__c", "SLAExpirationDate__c" }).iterator();
		Iterator<String> fieldTypes = Arrays.asList(new String[] { "id", "boolean",
				"reference", "string", "picklist", "reference", "textarea",
				"string", "string", "string", "string", "double", "double",
				"address", "textarea", "string", "string", "string", "string",
				"double", "double", "address", "phone", "phone", "string",
				"url", "url", "string", "picklist", "currency", "int",
				"picklist", "string", "textarea", "picklist", "string",
				"reference", "datetime", "reference", "datetime", "reference",
				"datetime", "date", "datetime", "datetime", "string", "string",
				"picklist", "picklist", "string", "string", "string", "string",
				"string", "string", "reference", "picklist", "picklist",
				"picklist", "double", "picklist", "string", "date" }).iterator();
		Map<String, String> metadata = new HashMap<String, String>();
		while (fieldnames.hasNext() && fieldTypes.hasNext()) {
			metadata.put(fieldnames.next(), fieldTypes.next());
		}
		Collection<BasicDBObject> data = SalesforceObjectHelper.readSalesforceXmlStreamToMongoObject(new FileInputStream(xml), metadata);
		org.junit.Assert.assertNotNull(data);
		org.junit.Assert.assertEquals(data.size(), 12);
		data.forEach(obj -> System.out.println(SalesforceObjectHelper.print(obj)));
	}
	
	@Test
	public void testReadSalesforceReportDataStreamToMongoObject() throws Exception {
		
		long tabularReportDuration = measureReportParsing("src/test/resources/tabularReport.json") / 1_000_000;
		long summaryReportDuration = measureReportParsing("src/test/resources/summaryReport.json") / 1_000_000;
		long matrixReportDuration = measureReportParsing("src/test/resources/matrixReport.json") / 1_000_000;
		System.out.println(String.format("Parsing tabular, summary, matrix reports successfully "
				+ "takes {%d}, {%d}, {%d} m-secs relevantly", tabularReportDuration, summaryReportDuration, matrixReportDuration));
	}
	
	private long measureReportParsing(String reportData) throws Exception {
		long start = System.nanoTime();
		Collection<BasicDBObject> data = SalesforceObjectHelper
				.readSalesforceReportDataStreamToMongoObject(new FileInputStream(new File(reportData)));
		org.junit.Assert.assertNotNull(data);
		//data.forEach(obj -> System.out.println(SalesforceObjectHelper.print(obj)));
		return System.nanoTime() - start;
	}
	
	public static void main(String[] args) throws Exception {
		String query = 
				//"$['factMap']['0!T']['rows'][*]['dataCells']";
				//"$['factMap'][*]['rows'][?(@.length > 1)]";
				"$['factMap'][*]['rows'][*]['dataCells']";
		Configuration conf = Configuration.defaultConfiguration();
		conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		DocumentContext document = JsonPath.using(conf).parse(new FileInputStream(new File("/data/salesforce/matrixReport-1.json")));
		List<List<Map<String, Object>>> jsonRows = document.read("$['factMap'][*]['rows'][*]['dataCells']");
		System.out.println(jsonRows.size());
		//System.out.println(document.read(query));
		//System.out.println(SalesforceObjectHelper.print(document.read(query)));
	}
}
