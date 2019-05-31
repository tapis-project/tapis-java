package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import java.util.TreeMap;

public class TapisSSHConnectionException 
 extends TapisRecoverableException 
{
    private static final long serialVersionUID = 6143550896805006147L;
    
	public TapisSSHConnectionException(String message, Throwable cause, TreeMap<String,String> state) 
	{
	    super(message, cause, state);
	}
}
