package edu.utexas.tacc.aloe.shared.exceptions;

public class FileEventProcessingException 
 extends AloeException
{
    private static final long serialVersionUID = -7467352287530703138L;
    
    public FileEventProcessingException(String message) {super(message);}
    public FileEventProcessingException(String message, Throwable cause) {super(message, cause);}
}
