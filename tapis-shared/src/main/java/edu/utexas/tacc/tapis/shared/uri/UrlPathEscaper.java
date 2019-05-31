package edu.utexas.tacc.tapis.shared.uri;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

/**
 * Handles the escaping of urls for use in generating URLS. This is 
 * preferred to {@link URLEncoder} and {@link URI} builders because 
 * it will handle UTF-8 encoding, spaces, !, etc. properly. 
 *  
 * @author dooley
 *
 */
public class UrlPathEscaper {
	/**
     * Takes a file or url path and properly URL escapes the string 
     * so it can be used as a valid url request. This is null safe.
     * 
     * @param plainPath
     * @return
     */
    public static String escape(String plainPath) {
    	
    	String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(StringUtils.stripToEmpty(plainPath), "/");
    	Escaper escaper = UrlEscapers.urlPathSegmentEscaper();
		
		List<String> encodedTokens = new ArrayList<String>();
		
		for (String token:tokens) {
			encodedTokens.add(escaper.escape(token));
		}
		
		return StringUtils.replace(StringUtils.join(encodedTokens, "/"), "+", "%2b");
    }
    
    /**
     * Takes a file or url path and properly URL escapes the string 
     * so it can be used as a valid url request. This is null safe.
     * 
     * @param plainPath
     * @return
     */
    public static String decode(String encodedPath) {
		try {
			return URLDecoder.decode(encodedPath, "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			return URLDecoder.decode(encodedPath);
		}
    }
}