package edu.utexas.tacc.aloe.shared.exceptions;

public class PermissionException 
 extends AloeException
{
    private static final long serialVersionUID = 4079086572081851519L;
    
    public PermissionException(String message) {super(message);}
    public PermissionException(String message, Throwable cause) {super(message, cause);}
}
