package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import java.util.TreeMap;

public class TapisQuotaException 
 extends TapisRecoverableException 
{
    private static final long serialVersionUID = -4247241252619345529L;
    
	public TapisQuotaException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
