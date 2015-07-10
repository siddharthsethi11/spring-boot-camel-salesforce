package com.github.deeprot.integration.camel.dto;

import java.io.Serializable;
import java.nio.charset.Charset;

import com.google.common.hash.Hashing;

/**
 * DTO class represents a Salesforce credentials object extracted from the Dataset
 * Also is a Camel Header embedded in the Exchange object.
 *  
 * @author bruce.nguyen
 *
 */
public class SalesforceCredentials implements Serializable {

	/**
	 * Generate UUID
	 */
	private static final long serialVersionUID = -4875666467593592646L;
	
	String email;
	String password;
	
	public SalesforceCredentials(String email, String password) {
		super();
		this.email = email;
		this.password = password;
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
	
	/**
	 * The unique hash number for the object, considerable as a primary key.
	 * Utilize the Guava library's <a href=
	 * "http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/hash/HashFunction.html"
	 * >hash function</a>.
	 * 
	 * @return the unique hash code
	 */
	public long uniqueHash() {
		return Hashing.md5().hashString(email + password, 
				Charset.forName("UTF-8")).asLong();
	}
}
