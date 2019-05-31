package edu.utexas.tacc.aloe.shared.exceptions.recoverable;

import java.util.TreeMap;

public class AloeQuotaException 
 extends AloeRecoverableException 
{
    private static final long serialVersionUID = -4247241252619345529L;
    
	public AloeQuotaException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
