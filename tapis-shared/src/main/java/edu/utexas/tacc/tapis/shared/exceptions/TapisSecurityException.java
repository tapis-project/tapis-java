package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisSecurityException 
 extends TapisException 
{
    private static final long serialVersionUID = -1308604776352625945L;
    
    public TapisSecurityException(String message) {super(message);}
	public TapisSecurityException(String message, Throwable cause) {super(message, cause);}
}
