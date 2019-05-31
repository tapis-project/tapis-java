package edu.utexas.tacc.tapis.shared.exceptions;

public class PermissionException 
 extends TapisException
{
    private static final long serialVersionUID = 4079086572081851519L;
    
    public PermissionException(String message) {super(message);}
    public PermissionException(String message, Throwable cause) {super(message, cause);}
}
