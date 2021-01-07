package edu.utexas.tacc.tapis.jobs.queue;

import java.util.Map;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueFilterException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class uses the ActiveMQ selector parser and evaluator to process
 * filters associated with our job queues.
 * 
 * @author rcardone
 */
public class SelectorFilter 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SelectorFilter.class);
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* parse:                                                                 */
    /* ---------------------------------------------------------------------- */
    public static BooleanExpression parse(String filter) throws JobQueueFilterException
    {
        // The filter should have been checked for obvious problems by the
        // time it gets here, but we want to avoid NPEs no matter what. 
        // Empty and blank strings trigger normal exception handling below.
        if (filter == null) {
            String msg = MsgUtils.getMsg("JOBS_QUEUE_EMPTY_FILTER");;
            _log.error(msg);
            throw new JobQueueFilterException(msg);
        }
        
        // Attempt to parse the filter.
        BooleanExpression expr = null;
        try {expr = SelectorParser.parse(filter);}
        catch (InvalidSelectorException e) {
            
            // Try to collect as much information as the parser makes available.
            String outermsg = e.getMessage();
            String innermsg = null;
            if (e.getCause() != null) innermsg = e.getCause().getMessage();
            
            // Create the new combined error message.
            String msg = "";
            if (outermsg != null) msg = outermsg;
            if (innermsg != null) {
                // Add spacing if we need it.
                if (!msg.isEmpty()) msg += " [";
                msg += innermsg;
                if (!msg.isEmpty()) msg += "]";
            }
            _log.error(MsgUtils.getMsg("JOBS_QUEUE_FILTER_PARSE_ERROR", msg), e);
            throw new JobQueueFilterException(msg, e);
        }
        
        return expr;
    }
    
    /* ---------------------------------------------------------------------- */
    /* match:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Determine whether the SQL-compliant filter expression evaluates to TRUE
     * given the specified property values.  The following URL provides details
     * on filter processing:
     * 
     *   http://docs.oracle.com/javaee/7/api/javax/jms/Message.html
     * 
     * The property values can only be class Boolean, Byte, Short, Integer, Long, 
     * Float, Double, and String; any other values will cause an exception.
     * Property names cannot be null or the empty string.
     *  
     * @param filter the non-null SQL expression to be evaluated
     * @param properties the key/value pairs used for substitution in the filter,
     *            can be null or empty
     * @return true if the filter evaluates to true, false otherwise
     * @throws JobQueueFilterException 
     */
    public static boolean match(String filter, Map<String, Object> properties) 
     throws JobQueueFilterException
    {
        // Parse the filter.  Null filters are checked in the called routine.
        BooleanExpression expr = parse(filter);
        
        // The easiest (and safest) way to evaluate a filter expression
        // using the provided key/value properties is to use the native
        // ActiveMQ data types and copy all properties to the message.
        //
        // Note that the property values can be be one of the primitive
        // type classes or String, but nothing else.
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        if (properties != null)
            try {message.setProperties(properties);}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("JOBS_QUEUE_FILTER_VALUE_ERROR", e.getMessage());
                 _log.error(msg, e);
                 throw new JobQueueFilterException(msg + " (" + e.getMessage() + ")");
             }
        
        // Set up the context that the evaluation code requires.
        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setMessageReference(message);
        
        // Evaluate the message with its properties.
        boolean result = false;
        try {result = expr.matches(ctx);}
         catch (Exception e) {
             String msg = MsgUtils.getMsg("JOBS_QUEUE_FILTER_EVAL_ERROR", e.getMessage());
             _log.error(msg, e);
             throw new JobQueueFilterException(msg + " (" + e.getMessage() + ")", e);
         }
        return result;
    }
}
