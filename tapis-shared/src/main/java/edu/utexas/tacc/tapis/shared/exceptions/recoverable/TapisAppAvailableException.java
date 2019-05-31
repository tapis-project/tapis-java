package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import java.util.TreeMap;

public class TapisAppAvailableException 
 extends TapisRecoverableException 
{
    private static final long serialVersionUID = -5112720649452406314L;
    
	public TapisAppAvailableException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
