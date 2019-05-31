package edu.utexas.tacc.tapis.shared.providers.email;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;
import edu.utexas.tacc.tapis.shared.providers.email.clients.LoggingEmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.clients.SMTPEmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.enumeration.EmailProviderType;

@Test(groups={"integration"})
public class EmailClientTest 
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // ==== PUT AN ACTUAL EMAIL ADDRESS HERE IF YOU WANT TO CHECK DELIVERY ====
    private static final String TARGET_EMAIL_ADDRESS = "someone@tacc.utexas.edu";
    
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* testSMTP:                                                                    */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void testSMTP() throws TapisException
    {
        // Get a client instance.
        SMTPEmailClient client = new SMTPEmailClient(new TestParameters());
        String body = "Plain text email body.";
        String htmlBody = "<h2>HTML email body</h2>";
        client.send("Walter White", TARGET_EMAIL_ADDRESS, "Just a test", body, htmlBody);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* testLog:                                                                     */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void testLog() throws TapisException
    {
        // Get a client instance.
        LoggingEmailClient client = new LoggingEmailClient(new TestParameters());
        String body = "Plain text email body.";
        String htmlBody = "<h2>HTML email body</h2>";
        client.send("Uncle Sam", TARGET_EMAIL_ADDRESS, "Just a test", body, htmlBody);
    }
    
    /* **************************************************************************** */
    /*                                 Parameter Class                              */
    /* **************************************************************************** */
    /** Class used to hardcode test parameters. */
    private static class TestParameters
     implements EmailClientParameters
    {
        @Override
        public EmailProviderType getEmailProviderType() {
            return EmailProviderType.SMTP;
        }

        @Override
        public boolean isEmailAuth() {
            return false;
        }

        @Override
        public String getEmailHost() {
            return "relay.tacc.utexas.edu";
        }

        @Override
        public int getEmailPort() {
            return 25;
        }

        @Override
        public String getEmailUser() {
            return "bud";
        }

        @Override
        public String getEmailPassword() {
            return "no-password";
        }

        @Override
        public String getEmailFromName() {
            return "Tapis Test";
        }

        @Override
        public String getEmailFromAddress() {
            return "no-reply@tacc.cloud";
        }
    }
}
