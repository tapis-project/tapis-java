package edu.utexas.tacc.tapis.shared.providers.email.clients;

import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;

/** Do nothing email client */
public final class NoneEmailClient 
 extends AbstractEmailClient
{
    // Constructor
    public NoneEmailClient(EmailClientParameters parms)
    {
        super(parms);
    }

    @Override
    public void send(String recipientName, String recipientAddress, String subject, String body, String htmlBody) {}
}
