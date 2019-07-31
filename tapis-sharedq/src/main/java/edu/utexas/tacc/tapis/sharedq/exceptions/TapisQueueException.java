package edu.utexas.tacc.tapis.sharedq.exceptions;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class TapisQueueException 
 extends TapisException 
{
    private static final long serialVersionUID = -8723317404072706425L;
    
    public TapisQueueException(String message) {super(message);}
	public TapisQueueException(String message, Throwable cause) {super(message, cause);}
}
