package edu.utexas.tacc.aloe.shared.exceptions.recoverable;

import edu.utexas.tacc.aloe.shared.exceptions.AloeException;

/** THIS CLASS DOES NOT YET SUBCLASS AloeRecoverableException because we haven't
 * implement recovery for database connection problems.  Currently, it's treated
 * like any other AloeException.  When recovery is implement for connection 
 * problems the superclass should be changed to AloeRecoverableException.
 * 
 * @author rcardone
 */
public class AloeDBConnectionException 
 extends AloeException 
{
    private static final long serialVersionUID = 8398712080848785211L;
    
	public AloeDBConnectionException(String message, Throwable cause) {super(message, cause);}
}
