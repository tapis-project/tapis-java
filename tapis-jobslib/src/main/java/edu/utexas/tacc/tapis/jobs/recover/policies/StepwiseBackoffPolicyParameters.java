package edu.utexas.tacc.tapis.jobs.recover.policies;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This class is a container for parameters for all the StepwiseBackoff 
 * policy class.  
 * 
 * @author rcardone
 */
public final class StepwiseBackoffPolicyParameters
 extends AbsPolicyParameters
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(StepwiseBackoffPolicyParameters.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
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
    private List<Pair<Integer,Long>> _steps;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public StepwiseBackoffPolicyParameters(JobRecovery jobRecovery) 
    {
        super(RecoverPolicyType.STEPWISE_BACKOFF, jobRecovery);
        initialize();
    }

    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* initialize:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Replace default field values with user-specified values if they exist. */
    private void initialize()
    {
        // -- Assign steps from a json serialization of a list of pairs.
        //    See the test program for an example of the correct json formatting.
        String value = _jobRecovery.getPolicyParameters().get("steps");
        if (value != null) {
            // The pair created here are mutable, though that should not 
            // be relied on since the default pairs are immutable.
            Gson gson = TapisGsonUtils.getGson(true);
            Type typeOfT = new TypeToken<ArrayList<MutablePair<Integer,Long>>>(){}.getType();
            try {_steps = gson.fromJson(value, typeOfT);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("JOB_RECOVERY_BAD_POLICY_PARM", 
                                                 "steps", value, e.getMessage());
                    _log.error(msg, e);
                }
        }

        // Assign steps if still null.
        if (_steps == null) _steps = getDefaultSteps();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDefaultSteps:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Default step configuration that should work for most uses.
     * 
     * @return the step list
     */
    private List<Pair<Integer,Long>> getDefaultSteps()
    {
        // Each step specifies a number of tries with the given delay in milliseconds.
        // Note that these pairs are immutable.
        ArrayList<Pair<Integer,Long>> steps = new ArrayList<>();
        steps.add(Pair.of(2,    10000L));   // 10 seconds
        steps.add(Pair.of(10,   20000L));   // 30 seconds
        steps.add(Pair.of(10,   60000L));   // 1 minute (default connection cutoff)
        steps.add(Pair.of(100, 120000L));   // 2 minutes 
        steps.add(Pair.of(-1,  300000L));   // 5 minutes forever
        
        return steps;
    }
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    public List<Pair<Integer, Long>> getSteps() {
        return _steps;
    }

    public void setSteps(List<Pair<Integer, Long>> steps) {
        this._steps = steps;
    }
}
