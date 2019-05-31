package edu.utexas.tacc.tapis.shared.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Generic class to validate ipv4 ip addresses.
 * 
 * @author adapted from mkyong
 */
public class IPAddressValidator 
{
    // Constants.
    private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
            + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
            + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
            + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
    
	private static final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

	/** Validate ip address with regular expression
	 * 
	 * @param ip address for validation
	 * @return true for valid ip address, false for invalid ip address
	 */
	public static boolean validate(final String ip) {
	    Matcher matcher = pattern.matcher(ip);
		return matcher.matches();
	}
}