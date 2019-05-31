package edu.utexas.tacc.tapis.shared.providers.email.clients;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

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
     throws TapisException 
    {   
        // Validate input.
        validateSendParms(recipientName, recipientAddress, subject, body, htmlBody);
        
        // Create the top level message object.
        JsonObject message = new JsonObject();
        
        // Create TO information.
        JsonObject to = new JsonObject();
        TapisGsonUtils.addTo(to, "name", recipientName);
        TapisGsonUtils.addTo(to, "address", recipientAddress);
        
        // Create FROM information.
        JsonObject from = new JsonObject();
        TapisGsonUtils.addTo(from, "name", _parms.getEmailFromName());
        TapisGsonUtils.addTo(from, "address", _parms.getEmailFromAddress());
        
        // Assemble the headers.
        JsonObject headers = new JsonObject();
        for (Entry<String,String> entry: getCustomHeaders().entrySet()) {
            TapisGsonUtils.addTo(headers, entry.getKey(), entry.getValue());
        }
        
        // Assemble the full message.
        message.add("headers", headers);
        message.add("to", to);
        message.add("from", from);
        TapisGsonUtils.addTo(message, "subject", subject);
        TapisGsonUtils.addTo(message, "body", body);
        TapisGsonUtils.addTo(message, "htmlBody", htmlBody);
        
        // Pretty print the information.
        Gson gson = TapisGsonUtils.getGson(true);
        _log.info("\n" + gson.toJson(message));
    }
}
