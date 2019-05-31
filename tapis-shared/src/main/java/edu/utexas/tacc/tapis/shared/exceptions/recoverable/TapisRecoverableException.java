package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import java.util.TreeMap;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class TapisRecoverableException
extends TapisException
{
    private static final long serialVersionUID = 8857182330433990489L;
    
    public TreeMap<String,String> state;
    
    public TapisRecoverableException(String message, TreeMap<String,String> state) 
    {
        super(message);
        this.state = state;
    }
    
    public TapisRecoverableException(String message, Throwable cause, TreeMap<String,String> state) 
    {
        super(message, cause);
        this.state = state;
    }

}
