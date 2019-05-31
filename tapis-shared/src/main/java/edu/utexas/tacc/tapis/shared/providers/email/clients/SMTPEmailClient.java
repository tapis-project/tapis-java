package edu.utexas.tacc.tapis.shared.providers.email.clients;

import java.util.Map.Entry;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;

/** SMTP email client */
public final class SMTPEmailClient 
 extends AbstractEmailClient
{
    // Constants
    private static Logger _log = LoggerFactory.getLogger(SMTPEmailClient.class);

    // Constructor
    public SMTPEmailClient(EmailClientParameters parms)
    {
        super(parms);
    }
    
    /** Send an email */
    @Override
    public void send(String recipientName, String recipientAddress, String subject, String body, String htmlBody)
     throws TapisException 
    {
        // Validate input.
        validateSendParms(recipientName, recipientAddress, subject, body, htmlBody);
        
        // Set smtp properties
        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", _parms.getEmailHost());
        props.put("mail.smtp.port", _parms.getEmailPort());
        props.put("mail.smtp.auth", Boolean.toString(_parms.isEmailAuth()));

        // Create the authenticator whether or not authentication is on.
        SMTPAuthenticator auth = new SMTPAuthenticator();
        Session mailSession = Session.getDefaultInstance(props, auth);
        
        // uncomment for debugging infos to stdout
        mailSession.setDebug(true);
        
        // Get the transport.
        Transport transport = null;
        try {transport = mailSession.getTransport();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_MAIL_TRANSPORT_ERROR", "smtp",
                                             _parms.getEmailHost(), _parms.getEmailPort(),
                                             _parms.isEmailAuth());
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
        
        // Assemble the message.
        MimeMessage message = null;
        try 
        {
            message = new MimeMessage(mailSession);
    
            Multipart multipart = new MimeMultipart("alternative");
    
            BodyPart part1 = new MimeBodyPart();
            part1.setText(body);
    
            BodyPart part2 = new MimeBodyPart();
            part2.setContent(htmlBody, "text/html");
    
            multipart.addBodyPart(part1);
            multipart.addBodyPart(part2);
    
            message.setContent(multipart);
            message.setSubject(subject);
            
            message.setFrom(new InternetAddress(
                    _parms.getEmailFromAddress(), _parms.getEmailFromName()));
            
            // add custom headers if present
            if (!getCustomHeaders().isEmpty()) {
                for (Entry<String,String> entry: getCustomHeaders().entrySet()) {
                    message.addHeader(entry.getKey(), entry.getValue());
                }
            }
            
            message.addRecipient(Message.RecipientType.TO,
                new InternetAddress(recipientAddress, recipientName));
        } 
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_MAIL_MESSAGE_CREATE", "smtp",
                                             _parms.getEmailHost(), _parms.getEmailPort(),
                                             recipientName, recipientAddress,
                                             e.getMessage());
                _log.error(msg, e);
                throw new TapisException(msg, e);
            } 
            finally {try {transport.close();} catch (Exception e) {} } // try to clean up 
        
        // Make the connection.
        try {transport.connect();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_MAIL_CONNECT", "smtp",
                        _parms.getEmailHost(), _parms.getEmailPort(),
                        recipientName, recipientAddress,
                        e.getMessage());
                _log.error(msg, e);
                try {transport.close();} catch (Exception e1) {}  // try to clean up
                throw new TapisException(msg, e);
            }
        
        // Send the mail.
        try {transport.sendMessage(message, message.getRecipients(Message.RecipientType.TO));}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_MAIL_SEND", "smtp",
                        _parms.getEmailHost(), _parms.getEmailPort(),
                        recipientName, recipientAddress,
                        e.getMessage());
                _log.error(msg, e);
                try {transport.close();} catch (Exception e1) {}  // try to clean up
                throw new TapisException(msg, e);
            }
        
        // Close the transport.
        try {transport.close();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_MAIL_CLOSE", "smtp",
                        _parms.getEmailHost(), _parms.getEmailPort(),
                        recipientName, recipientAddress,
                        e.getMessage());
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
    }

    // Convenience authenticator class.
    private class SMTPAuthenticator 
     extends Authenticator
    {
      public PasswordAuthentication getPasswordAuthentication() {
         String username = _parms.getEmailUser();
         String password = _parms.getEmailPassword();
         return new PasswordAuthentication(username, password);
      }
    }
}
