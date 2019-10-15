package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisImplException
 extends TapisException
{
    private static final long serialVersionUID = -8163399520088986524L;
    
    // Condition codes allow backend code to communicate the type of the 
    // error to the frontend might reflect.  The frontend has complete
    // discretion on what it reports to the user, but the intention is that
    // condition codes provide a 1-to-1 mapping to frontend error codes.
    //
    // Use TapisNotFoundException for missing data errors.
    public enum Condition {
        INTERNAL_SERVER_ERROR,
        BAD_REQUEST
    }
    
    // The condition code should always be set. 
    public final Condition condition;
    
    public TapisImplException(String message, Condition cond) 
    {super(message); condition = cond;}
    public TapisImplException(String message, Throwable cause, Condition cond) 
    {super(message, cause); condition = cond;}

}
