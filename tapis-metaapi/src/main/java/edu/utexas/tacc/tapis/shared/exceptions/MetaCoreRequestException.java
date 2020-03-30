package edu.utexas.tacc.tapis.shared.exceptions;

public class MetaCoreRequestException
 extends TapisException
{
    private static final long serialVersionUID = 477391359506425327L;
    
    public MetaCoreRequestException(String message) {super(message);}
    public MetaCoreRequestException(String message, Throwable cause) {super(message, cause);}
}
