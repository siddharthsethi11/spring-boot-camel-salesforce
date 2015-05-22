package com.hrboss.model;

public class DataSource {

	DatasourceType dsType;
	String email;
	String password;
	String clientKey;
	String clientSecureKey;

	public DatasourceType getType() {
		return dsType;
	}

	public void setType(DatasourceType dsType) {
		this.dsType = dsType;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getClientKey() {
		return clientKey;
	}

	public void setClientKey(String clientKey) {
		this.clientKey = clientKey;
	}

	public String getClientSecureKey() {
		return clientSecureKey;
	}

	public void setClientSecureKey(String clientSecureKey) {
		this.clientSecureKey = clientSecureKey;
	}
}