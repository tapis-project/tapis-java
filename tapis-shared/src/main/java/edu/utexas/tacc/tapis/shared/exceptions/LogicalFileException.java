package edu.utexas.tacc.tapis.shared.exceptions;

public class LogicalFileException 
 extends AloeException
{
    private static final long serialVersionUID = -4631735455285433465L;
    
    public LogicalFileException(String message) {super(message);}
    public LogicalFileException(String message, Throwable cause) {super(message, cause);}
}
