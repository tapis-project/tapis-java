package edu.utexas.tacc.tapis.shared.providers.email;

import edu.utexas.tacc.tapis.shared.providers.email.enumeration.EmailProviderType;

public interface EmailClientParameters 
{
    EmailProviderType getEmailProviderType();

    boolean isEmailAuth();
    
    String getEmailHost();

    int getEmailPort();

    String getEmailUser();

    String getEmailPassword();

    String getEmailFromName();
    
    String getEmailFromAddress();
}
