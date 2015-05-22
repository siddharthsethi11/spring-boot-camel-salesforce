package com.hrboss.integration.helper;

import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.springframework.util.Assert;

import com.hrboss.integration.camel.dto.SalesforceCredentials;
import com.hrboss.model.DataSource;

public class SalesforceLoginConfigHelper {

	private static final boolean LAZY_LOGIN = true;

	public static SalesforceLoginConfig getLoginConfig(DataSource dataSource)
			throws IllegalArgumentException {
		Assert.notNull(dataSource);
		Assert.notNull(dataSource.getClientKey());
		Assert.notNull(dataSource.getClientSecureKey());
		Assert.notNull(dataSource.getEmail());
		Assert.notNull(dataSource.getPassword());
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				dataSource.getClientKey(), dataSource.getClientSecureKey(),
				dataSource.getEmail(), dataSource.getPassword(), LAZY_LOGIN);
		return config;
	}
	
	public static SalesforceLoginConfig getLoginConfig(SalesforceCredentials creds)
			throws IllegalArgumentException {
		Assert.notNull(creds);
		Assert.notNull(creds.getClientKey());
		Assert.notNull(creds.getClientSecureKey());
		Assert.notNull(creds.getEmail());
		Assert.notNull(creds.getPassword());
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				creds.getClientKey(), creds.getClientSecureKey(),
				creds.getEmail(), creds.getPassword(), LAZY_LOGIN);
		return config;
	}
	
	public static SalesforceCredentials getCredentials(DataSource dataSource) {
		Assert.notNull(dataSource);
		Assert.notNull(dataSource.getClientKey());
		Assert.notNull(dataSource.getClientSecureKey());
		Assert.notNull(dataSource.getEmail());
		Assert.notNull(dataSource.getPassword());
		final SalesforceCredentials creds = new SalesforceCredentials(
				dataSource.getClientKey(), dataSource.getClientSecureKey(),
				dataSource.getEmail(), dataSource.getPassword());
		return creds;
	}
	
	public static SalesforceLoginConfig getDefaultLogin() {
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				"3MVG9I1kFE5Iul2BLhYUBv2s5B6ndxx8LPJecj5cBYNkD9DDrqeL3Sm7LQ6REzZ6vb4MvWG9G65rxXYxGLHyr", "6630656843211848500",
				//"bernie.schiemer@hiringboss.com", "Remember12", LAZY_LOGIN);
				"bruce.nguyen@hiringboss.com", "Hrb0ss2015", LAZY_LOGIN);
		return config;
	}
}
