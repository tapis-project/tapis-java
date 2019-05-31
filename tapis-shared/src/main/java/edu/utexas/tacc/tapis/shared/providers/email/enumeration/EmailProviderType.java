package edu.utexas.tacc.tapis.shared.providers.email.enumeration;

/**
 * Types of email providers supported
 * @author dooley
 *
 */
/**
 * @author dooley
 *
 */
public enum EmailProviderType {

	/**
	 * Sends email via smtp 
	 */
	SMTP,
	
	/**
	 * Writes email to log file 
	 */
	LOG,
	
	/**
	 * Ignores emails entirely 
	 */
	NONE
}
