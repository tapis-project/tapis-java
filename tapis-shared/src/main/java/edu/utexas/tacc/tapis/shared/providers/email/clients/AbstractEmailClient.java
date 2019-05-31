package edu.utexas.tacc.tapis.shared.providers.email.clients;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;

/** Base class for email clients.
 * 
 * @author rcardone
 */
public abstract class AbstractEmailClient 
 implements EmailClient 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AbstractEmailClient.class);

    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
    // Fields shared with subclasses.
    protected final EmailClientParameters _parms;
    protected Map<String, String>         _customHeaders = new HashMap<String, String>();
    
    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected AbstractEmailClient(EmailClientParameters parms)
    {
        _parms = parms;
    }
    
    /* **************************************************************************** */
    /*                              Protected Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getCustomHeaders:                                                            */
    /* ---------------------------------------------------------------------------- */
    protected void validateSendParms(String recipientName, String recipientAddress, 
                                     String subject, String body, String htmlBody)
     throws TapisException
    {
        if (StringUtils.isBlank(recipientName)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateSendParms", "recipientName");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(recipientAddress)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateSendParms", "recipientAddress");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(body)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateSendParms", "body");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(htmlBody)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateSendParms", "htmlBody");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(subject)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateSendParms", "subject");
            _log.error(msg);
            throw new TapisException(msg);
        }
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getCustomHeaders:                                                            */
    /* ---------------------------------------------------------------------------- */
    @Override
    public synchronized Map<String, String> getCustomHeaders() {return _customHeaders;}

    /* ---------------------------------------------------------------------------- */
    /* setCustomHeaders:                                                            */
    /* ---------------------------------------------------------------------------- */
    @Override
    public synchronized void setCustomHeaders(Map<String, String> customHeaders) {
        this._customHeaders = customHeaders;
    }

}   
