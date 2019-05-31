package edu.utexas.tacc.aloe.shared.exceptions.recoverable;

import java.util.TreeMap;

public class AloeSystemAvailableException 
 extends AloeRecoverableException 
{
    private static final long serialVersionUID = 4179443742417959182L;
    
	public AloeSystemAvailableException(String message, TreeMap<String,String> state) 
	{
	    super(message, state);
	}
}
