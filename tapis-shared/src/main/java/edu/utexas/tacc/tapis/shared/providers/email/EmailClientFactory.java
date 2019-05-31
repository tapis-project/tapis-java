package edu.utexas.tacc.tapis.shared.providers.email;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.clients.LoggingEmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.clients.NoneEmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.clients.SMTPEmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.enumeration.EmailProviderType;

public class EmailClientFactory 
{
    // Error reporting.
    private static final Logger _log = LoggerFactory.getLogger(EmailClientFactory.class);

    /** Create a new email client of the type specified in the parameter object.
     * 
     * @param parms parameters needed to instantiate any email client object
     * @return an EmailClient of the specified type
     * @throws TapisRuntimeException if no client can be found.
     */
    public static EmailClient getClient(EmailClientParameters parms)
     throws TapisException
    {
        EmailProviderType provider = parms.getEmailProviderType();
        switch (provider)
        {
            case SMTP: return new SMTPEmailClient(parms);
            case LOG:  return new LoggingEmailClient(parms);
            case NONE: return new NoneEmailClient(parms);
            
            default:
                String msg = MsgUtils.getMsg("TAPIS_MAIL_UNKNOWN_CLIENT", provider);
                _log.error(msg);
                throw new TapisRuntimeException(msg);
        }
    }
}
