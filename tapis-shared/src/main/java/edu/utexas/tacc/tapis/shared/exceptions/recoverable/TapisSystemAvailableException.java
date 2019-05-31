package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import java.util.TreeMap;

public class TapisSystemAvailableException 
 extends TapisRecoverableException 
{
    private static final long serialVersionUID = 4179443742417959182L;
    
	public TapisSystemAvailableException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
