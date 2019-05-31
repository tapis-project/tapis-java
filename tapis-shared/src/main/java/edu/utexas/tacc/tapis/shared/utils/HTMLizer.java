package edu.utexas.tacc.tapis.shared.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Converts standard text to HTML, activating hyperlinks
 * @author dooley
 *
 */
public class HTMLizer {
    
    private static final Pattern urlPattern = Pattern.compile(
            "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                    + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                    + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
 
    private static final Pattern emailPattern = Pattern.compile(
            "\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,4}\\b",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    
    /**
     * Extracts all URL from a string, exclusive of email addresses
     * @param s string to search
     * @return list of unformatted email addresses
     */
    public static List<String> extractUrlFromString(String s) {
        Matcher matcher = urlPattern.matcher(s);
        List<String> matches = new ArrayList<String>();
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        
        return matches;
    }
    
    /**
     * Extracts all email addresses from a string
     * @param s string to search
     * @return list of unformatted email addresses
     */
    public static List<String> extractEmailFromString(String s) {
        Matcher matcher = emailPattern.matcher(s);
        List<String> matches = new ArrayList<String>();
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        return matches;
    }
    
    public static String htmlize(String text) {
        
        String html = text;
        for (String url: extractUrlFromString(text)) {
            html = StringUtils.replace(html, url, anchorUrl(url));
        }
        
        for (String email: extractEmailFromString(text)) {
            html = StringUtils.replace(html, email, anchorEmail(email));
        }
        
        html.replaceAll("\\n\\n", "</p><br><p>");
        
        html.replaceAll("\\n", "<br>");
        
        return "<p>" + html + "</p>";
    }

    private static String anchorUrl(String url) {
        return "<a href=\"" + url + "\">" + url + "</a>";
    }
    
    private static String anchorEmail(String url) {
        return "<a href=\"mailto:" + url + "\">" + url + "</a>";
    }

}
