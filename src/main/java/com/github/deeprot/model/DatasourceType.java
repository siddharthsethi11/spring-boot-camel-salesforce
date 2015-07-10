package com.github.deeprot.model;

public enum DatasourceType {
	
	DATABASE, FILE, SALESFORCE;
	
	public boolean equals(String dsType) {
		return this.toString().equals(dsType);
	}

}
