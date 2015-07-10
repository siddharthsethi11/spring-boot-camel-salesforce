package com.github.deeprot.integration.helper;

import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.springframework.util.Assert;

import com.github.deeprot.integration.camel.dto.SalesforceCredentials;
import com.github.deeprot.model.DataSource;

/**
 * Helper class to extract the Salesforce credentials from different DTOs
 * 
 * @author bruce.nguyen
 *
 */
public class SalesforceLoginConfigHelper {

	private static final boolean LAZY_LOGIN = true;

	public static SalesforceLoginConfig getLoginConfig(DataSource dataSource)
			throws IllegalArgumentException {
		Assert.notNull(dataSource);
		Assert.notNull(dataSource.getEmail());
		Assert.notNull(dataSource.getDBPassword());
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				null, null,
				dataSource.getEmail(), dataSource.getDBPassword(), LAZY_LOGIN);
		return config;
	}
	
	public static SalesforceLoginConfig getLoginConfig(SalesforceCredentials creds)
			throws IllegalArgumentException {
		Assert.notNull(creds);
		Assert.notNull(creds.getEmail());
		Assert.notNull(creds.getPassword());
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				null, null,
				creds.getEmail(), creds.getPassword(), LAZY_LOGIN);
		return config;
	}
	
	public static SalesforceCredentials getCredentials(DataSource dataSource) {
		Assert.notNull(dataSource);
		Assert.notNull(dataSource.getEmail());
		Assert.notNull(dataSource.getDBPassword());
		final SalesforceCredentials creds = new SalesforceCredentials(dataSource.getEmail(), dataSource.getDBPassword());
		return creds;
	}
	
	public static SalesforceLoginConfig getDefaultLogin() {
		final SalesforceLoginConfig config = new SalesforceLoginConfig(
				SalesforceLoginConfig.DEFAULT_LOGIN_URL,
				"3MVG9I1kFE5Iul2BLhYUBv2s5B6ndxx8LPJecj5cBYNkD9DDrqeL3Sm7LQ6REzZ6vb4MvWG9G65rxXYxGLHyr", "6630656843211848500",
				"bruce.nguyen@hiringboss.com", "Hrb0ss2015", LAZY_LOGIN);
		return config;
	}
}
