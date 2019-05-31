package edu.utexas.tacc.tapis.shared.exceptions.recoverable;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** THIS CLASS DOES NOT YET SUBCLASS TapisRecoverableException because we haven't
 * implement recovery for database connection problems.  Currently, it's treated
 * like any other TapisException.  When recovery is implement for connection 
 * problems the superclass should be changed to TapissRecoverableException.
 * 
 * @author rcardone
 */
public class TapisDBConnectionException 
 extends TapisException 
{
    private static final long serialVersionUID = 8398712080848785211L;
    
	public TapisDBConnectionException(String message, Throwable cause) {super(message, cause);}
}
