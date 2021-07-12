package edu.utexas.tacc.tapis.jobs.utils;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoverableException;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobUtils.class);

    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap non-tapis exceptions in a TapisException keeping the same error message
     * in the wrapped exception.  Recoverable exception types are handled so that
     * their type is preserved. 
     * 
     * @param e any throwable that we might wrap in a tapis exception
     * @return a TapisException
     */
    public static TapisException tapisify(Throwable e)
    {
        return tapisify(e, null);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap non-tapis exceptions in a TapisException keeping the same error message
     * in the wrapped exception. Recoverable exception types are handled so that
     * their type is preserved. 
     * 
     * Note that JobAsyncCmdExceptions are unchecked exceptions that do not inherit
     * from TapisException and are designed to stop or pause job processing.  As 
     * such they should NOT be passed into this method.  If one is passed in, we
     * simply rethrow it so that its effect on job processing will still occur.
     * 
     * @param e any throwable that we might wrap in a tapis exception
     * @return a TapisException
     */
    public static TapisException tapisify(Throwable e, String msg)
    {
        // Dynamic binding is not used for static methods or in
        // overloaded methods, so we have to explicitly select 
        // the recoverable exception type method here.
        //
        // JobAsyncCmdExceptions should NOT be sent here but we 
        // handle that case anyway. 
        if (e instanceof JobRecoverableException) 
            return JobUtils.tapisify((JobRecoverableException)e, msg);
        else if (e instanceof JobAsyncCmdException) throw (JobAsyncCmdException)e;
        else if (e instanceof TapisDBConnectionException) return (TapisDBConnectionException)e;
        else return TapisUtils.tapisify(e, msg);
    }

    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap JobRecoverableException exceptions in a new version of themselves if
     * msg is non-null.  Otherwise, just return the original exception.
     * 
     * @param e any non-null JobRecoverableException that we might wrap in
     * @param msg the new message or null
     * @return a JobRecoverableException
     */
    private static JobRecoverableException tapisify(JobRecoverableException e, String msg)
    {
        // No need to change anything.  Note this method
        // must only be called when e is not null.
        if (msg == null) return e;
        
        // The result exception.
        JobRecoverableException recoveryException = null;
        
        // Create a new instance of the same tapis exeception type.
        Class<?> cls = e.getClass();
                  
        // Get the two argument (recoveryMsg, msg, cause) constructor that all 
        // JobRecoverableException subtypes implement.
        Class<?>[] parameterTypes = {JobRecoverMsg.class, String.class, Throwable.class};
        Constructor<?> cons = null;
        try {cons = cls.getConstructor(parameterTypes);}
            catch (Exception e2) {
                String msg2 = MsgUtils.getMsg("TAPIS_REFLECTION_ERROR", cls.getName(), 
                                              "getConstructor", e.getMessage());
                _log.error(msg2, e2);
            }
                  
        // Use the constructor to assign the result variable.
        if (cons != null) {
            try {recoveryException = (JobRecoverableException) cons.newInstance(
                                        e.jobRecoverMsg, msg == null ? e.getMessage() : msg, e);}
            catch (Exception e2) {
                String msg2 = MsgUtils.getMsg("TAPIS_REFLECTION_ERROR", cls.getName(), 
                                              "newInstance", e.getMessage());
                _log.error(msg2, e2);
            }
        } 
                  
        // If unable to create a wrapper exception just return the original one since
        // JobRecoverableException cannot be used as a fallback since it's abstract. 
        if (recoveryException == null) recoveryException = e;
        
        // Never null.
        return recoveryException;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getLastLine:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Get all characters after the last newline character is a string.  The string
     * must be non-null and must already be trimmed of leading and trailing whitespace.
     * This method is useful in stripping the banner information from the output of
     * remote commands. 
     * 
     * @param s the remote result string
     * @return the last line of the string
     */
    public static String getLastLine(String s)
    {
        // The input is a non-null, trimmed string so
        // a non-negative index must be at least one
        // character from the end of the string.
        int index = s.lastIndexOf('\n');
        if (index < 0) return s;
        return s.substring(index + 1);
    }

}
