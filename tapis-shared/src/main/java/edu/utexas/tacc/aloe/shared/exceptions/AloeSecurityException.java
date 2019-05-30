package edu.utexas.tacc.aloe.shared.exceptions;

public class AloeSecurityException 
 extends AloeException 
{
    private static final long serialVersionUID = -1308604776352625945L;
    
    public AloeSecurityException(String message) {super(message);}
	public AloeSecurityException(String message, Throwable cause) {super(message, cause);}
}
