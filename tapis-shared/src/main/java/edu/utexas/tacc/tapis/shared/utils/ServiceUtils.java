package edu.utexas.tacc.tapis.shared.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.EmailValidator;
import org.json.JSONStringer;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dooley
 * 
 */
public class ServiceUtils 
{
    private static final Logger log = LoggerFactory.getLogger(ServiceUtils.class);
    
    // Only calculate this stuff once.
    private static final String phoneNumberRegex = "^\\(?(\\d{3})\\)?[- ]?(\\d{3})[- ]?(\\d{4})$";
    private static final Pattern phoneNumberPattern = Pattern.compile(phoneNumberRegex, Pattern.CASE_INSENSITIVE);
	
	public static boolean isValidEmailAddress(String value)
	{
		if (StringUtils.isEmpty(value)) {
			return false;
		} else {
			return org.apache.commons.validator.routines.EmailValidator.getInstance(false).isValid(value);
		}
	}

	public static boolean isValidPhoneNumber(String value)
	{
		if (StringUtils.isBlank(value)) return false;
		Matcher matcher = phoneNumberPattern.matcher(value);
		return matcher.matches();
	}


	/**
	 * Used to wrap all output from the service in a JSON object with two
	 * attributes "status" and "message." the former is success or failure. The
	 * latter is the output from a service invocation or the error message.
	 * 
	 * @param status
	 * @param json
	 * @return
	 */
	private static String wrapOutput(String status, String message, String json)
	{
		JSONWriter writer = new JSONStringer();
		try
		{
			writer.object().key("status").value(status).key("message").value(
					message).key("result").value(json).endObject();
		}
		catch (Exception e)
		{
			try
			{
				writer.object().key("status").value("error").key("message")
						.value(e.getMessage()).endObject();
			}
			catch (Exception e1)
			{
			}
		}
		return writer.toString();
	}

	/**
	 * Used to wrap the payload of a service invocation in a json object
	 * 
	 * @param message
	 * @param json
	 * @return
	 */
	public static String wrapSuccess(String message, String json)
	{
		return wrapOutput("success", null, json);
	}

	/**
	 * Used to wrap all output from the service in a JSON object with two
	 * attributes "status" and "message." the former is success or failure. The
	 * latter is the output from a service invocation or the error message.
	 * 
	 * @param status
	 * @param json
	 * @return
	 */
	public static String wrapSuccess(String json)
	{
		return wrapSuccess(null, json);
	}

	/**
	 * Used to wrap all output from the service in a JSON object with two
	 * attributes "status" and "message." the former is success or failure. The
	 * latter is the output from a service invocation or the error message.
	 * 
	 * @param status
	 * @param json
	 * @return
	 */
	public static String wrapError(String message, String json)
	{
		return wrapOutput("error", message, null);
	}

	public static String stripSurroundingBrackets(String str) 
	{
		if (str.startsWith("[")) str = str.substring(1).trim();
		if (str.equals("]"))
			str = null;
		else {
			if (str.endsWith("]")) str = str.substring(0, str.lastIndexOf("]") - 1).trim();
			str = str.replaceAll("\"", "");
		}
		
		return str;
	}
	
	public static boolean isEmailAddress(String endpoint)
	{
		return EmailValidator.getInstance().isValid(endpoint);
	}
	
	public static String explode(String glue, List<?> list)
	{
		
		String explodedList = "";
		
		if (list != null && !list.isEmpty()) return explodedList;
		
		for(Object field: list) 
		{
			explodedList += glue + field.toString();
		}	
		
		return explodedList.substring(glue.length());
	}
	
	public static String[] implode(String separator, String tags)
	{
		if (StringUtils.isBlank(tags)) 
		{
			return new String[]{""};
		}
		else if (!tags.contains(separator))
		{
			return new String[]{tags};
		}
		else
		{
			return StringUtils.split(tags, separator);
		}
	}
	
	/**
	 * Formats a 10 digit phone number into (###) ###-#### format
	 * 
	 * @param phone
	 * @return formatted phone number string
	 */
	public static String formatPhoneNumber(String phone) 
	{	
		if (StringUtils.isEmpty(phone)) { 
			return null;
		}
		else 
		{
			phone = phone.replaceAll("[^\\d.]", "");
			return String.format("(%s) %s-%s", 
					phone.substring(0, 3), 
					phone.substring(3, 6), 
					phone.substring(6, 10));
		}
	}
	
    /**
     * Wraps a string in properly escaped quotation marks.
     * @param s
     * @return
     */
    public static String enquote(String s) {
        s = StringUtils.strip(s, "\"");
        if (StringUtils.isEmpty(s)) {
            s = "";
        } 
                
        return "\"" + s + "\"";
    }
    
	/**
	 * Returns the current local IP address or an empty string in error case /
	 * when no network connection is up.
	 * <p>
	 * The current machine could have more than one local IP address so might
	 * prefer to use {@link #getAllLocalIPs() } or
	 * {@link #getAllLocalIPs(java.lang.String) }.
	 * <p>
	 * If you want just one IP, this is the right method and it tries to find
	 * out the most accurate (primary) IP address. It prefers addresses that
	 * have a meaningful dns name set for example.
	 * 
	 * @return Returns the current local IP address or an empty string in error
	 *         case.
	 * @since 0.1.0
	 */
	public static String getLocalIP()
	{
		String ipOnly = "";
		try
		{
			Enumeration<NetworkInterface> nifs = NetworkInterface
					.getNetworkInterfaces();
			if (nifs == null)
				return "";
			while (nifs.hasMoreElements())
			{
				NetworkInterface nif = nifs.nextElement();
				// We ignore subinterfaces - as not yet needed.

				if (!nif.isLoopback() && nif.isUp() && !nif.isVirtual())
				{
					Enumeration<InetAddress> adrs = nif.getInetAddresses();
					while (adrs.hasMoreElements())
					{
						InetAddress adr = adrs.nextElement();
						if (adr != null
								&& !adr.isLoopbackAddress()
								&& ( nif.isPointToPoint() || !adr
										.isLinkLocalAddress() ))
						{
							String adrIP = adr.getHostAddress();
							String adrName;
							if (nif.isPointToPoint()) // Performance issues getting hostname for mobile internet sticks
								adrName = adrIP;
							else
								adrName = adr.getCanonicalHostName();

							if (!adrName.equals(adrIP))
								return adrIP;
							else
								ipOnly = adrIP;
						}
					}
				}
			}
			return ipOnly;
		}
		catch (SocketException ex)
		{
			return "";
		}
	}

	public static String formatPhoneNumberForSMS(String phone) 
	{
		phone = phone.replaceAll("[^\\d]", "");
		return "+1" + phone;
	}

    /**
     * Checks whether the url matches a pushpin url structure.
     * 
     * @param callbackUrl
     * @return
     */
    public static boolean isValidPushpinChannel(String callbackUrl) {
        
        if (StringUtils.isEmpty(callbackUrl)) {
        	return false;
        }
        // fanout.io realm
        else {
        	return callbackUrl.matches("^(?:http|https)://([a-zA-Z0-9]+)(\\.[a-zA-Z0-9]+)*:([\\d]{2,5})/publish$");
    	}
    }
    
    /**
     * Checks whether the url matches a fanout.io realm subdomain structure.
     * 
     * @param callbackUrl
     * @return
     */
    public static boolean isValidFanoutIoChannel(String callbackUrl) {
        
        if (StringUtils.isEmpty(callbackUrl)) {
        	return false;
        }
        // fanout.io realm
        else {
        	return callbackUrl.matches("^(?:http|https)://([a-zA-Z0-9]+).fanoutcdn.com/(fpp|bayoux)");
    	}
    }
}
