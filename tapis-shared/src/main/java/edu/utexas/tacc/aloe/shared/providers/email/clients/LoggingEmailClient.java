package edu.utexas.tacc.aloe.shared.providers.email.clients;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.aloe.shared.exceptions.AloeException;
import edu.utexas.tacc.aloe.shared.providers.email.EmailClientParameters;
import edu.utexas.tacc.aloe.shared.utils.AloeGsonUtils;

/**
 * Email client used for testing to write emails to log output rather than
 * send them.
 *
 */
public final class LoggingEmailClient 
 extends AbstractEmailClient
{
    // Constants
    private static Logger _log = LoggerFactory.getLogger(LoggingEmailClient.class);
    
    // Constructor
    public LoggingEmailClient(EmailClientParameters parms)
    {
        super(parms);
    }
    
    @Override
    public void send(String recipientName, String recipientAddress, String subject, String body, String htmlBody)
     throws AloeException 
    {   
        // Validate input.
        validateSendParms(recipientName, recipientAddress, subject, body, htmlBody);
        
        // Create the top level message object.
        JsonObject message = new JsonObject();
        
        // Create TO information.
        JsonObject to = new JsonObject();
        AloeGsonUtils.addTo(to, "name", recipientName);
        AloeGsonUtils.addTo(to, "address", recipientAddress);
        
        // Create FROM information.
        JsonObject from = new JsonObject();
        AloeGsonUtils.addTo(from, "name", _parms.getEmailFromName());
        AloeGsonUtils.addTo(from, "address", _parms.getEmailFromAddress());
        
        // Assemble the headers.
        JsonObject headers = new JsonObject();
        for (Entry<String,String> entry: getCustomHeaders().entrySet()) {
            AloeGsonUtils.addTo(headers, entry.getKey(), entry.getValue());
        }
        
        // Assemble the full message.
        message.add("headers", headers);
        message.add("to", to);
        message.add("from", from);
        AloeGsonUtils.addTo(message, "subject", subject);
        AloeGsonUtils.addTo(message, "body", body);
        AloeGsonUtils.addTo(message, "htmlBody", htmlBody);
        
        // Pretty print the information.
        Gson gson = AloeGsonUtils.getGson(true);
        _log.info("\n" + gson.toJson(message));
    }
}
