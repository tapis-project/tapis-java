package edu.utexas.tacc.aloe.shared.exceptions.recoverable;

import java.util.TreeMap;

public class AloeAppAvailableException 
 extends AloeRecoverableException 
{
    private static final long serialVersionUID = -5112720649452406314L;
    
	public AloeAppAvailableException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
