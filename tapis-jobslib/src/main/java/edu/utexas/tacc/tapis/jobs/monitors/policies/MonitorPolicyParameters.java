package edu.utexas.tacc.tapis.jobs.monitors.policies;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/** This class is a container for parameters for all monitor policy
 * concrete classes.  As such, it represents the union of all possible
 * parameters for all policy classes.  The policy classes can use the 
 * same parameters and it's up the caller to set the appropriate 
 * parameters. 
 * 
 * @author rcardone
 */
public final class MonitorPolicyParameters 
{
    // ----------------- StepwiseBackoff parameters -----------------
    // Steps are arranged in order from first to last.  The left value
    // indicates the number of times this step should be tried.  The
    // right value is the number of millisecond between each try.
    //
    // A zero or negative left value means try forever, which only makes
    // sense in the last step.  The right value cannot be negative.
    //
    // A default list of steps will be provided if this parameter is 
    // null or the list is empty.
    List<Pair<Integer,Long>> steps;
    
    // This parameter limits the maximum number of seconds between the
    // first try and the last try.  This value provides a way to 
    // cap the amount of real time that the policy executes no matter
    // how the steps are configured.  
    //
    // A value of zero or less disables this parameter.
    long maxElapsedSeconds;
    
    // Steps with wait times this size or greater will close their
    // connection to the remote system after each status check.  The
    // goal is to avoid the connection setup/teardown overhead on
    // status calls that might be issued in quick succession.
    long stepConnectionCloseMillis = MonitorPolicy.DEFAULT_STEP_CONN_CLOSE_MS;
    
    // The maximum number of minutes in which an unbroken sequence of 
    // monitoring attempts failures causes a timeout.
    long maxConsecutiveFailureMinutes = MonitorPolicy.DEFAULT_CONSECUTIVE_FAILURE_MINUTES; 
}
