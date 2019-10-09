package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisNotFoundException 
 extends TapisException 
{
    private static final long serialVersionUID = 6546186422079053585L;
    
    // Name not found.
    public String missingName;
    
    public TapisNotFoundException(String message, String name) 
     {super(message); missingName = name;}
	public TapisNotFoundException(String message, Throwable cause, String name) 
	 {super(message, cause); missingName = name;}
}
