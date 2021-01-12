package edu.utexas.tacc.tapis.jobs.statemachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.Persister;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.StateActionPair;
import org.statefulj.fsm.model.Transition;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This specialization of StatefulJ's FSM class adds strict mode processing
 * of events and disables transition retries.  The main interface to this
 * class is through the public methods of JobFSMUtils.  Code outside this 
 * package should probably not be calling the state machine directly. 
 * 
 * When in effect, strict mode will cause an IllegalStateException to be thrown
 * when an event occurs for which the current state does not have an explicit 
 * transition defined for that event.  When strict mode is not in effect, such
 * events are simply ignored.
 * 
 * This state machine will not block on transitions nor will it attempt to retry
 * transitions.
 * 
 * Run JobFSMTest to test every possible state transition. 
 * 
 * @author rcardone
 *
 * @param <T> a class that implements the persister interface
 */
public class JobFSM<T extends IJobFSMStatefulEntity>
 extends FSM<T>
{
    /* ********************************************************************** */
    /*                                Constants                               */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobFSM.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // When the strict field is true, IllegalStateException's are thrown when
    // an event occurs and the current state does not have an explicit transition
    // defined for the event.  When this field is false, the default FSM 
    // behavior is to ignore the event.
    private boolean _strict;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /**
     * FSM Constructor with the name of the FSM and Persister responsible 
     * for setting the State on the Entity.  This the strict parameter extends
     * the capabilities of the superclass.
     *
     * @param name Name associated with the FSM
     * @param persister Persister responsible for setting the State on the Entity
     * @param strict if true, throw an exception if a transition is not defined
     *         on the current for the current event.  If false, implement the 
     *         default noop behavior. 
     */
    public JobFSM(String name, Persister<T> persister, boolean strict) 
    {
        // Set fields.
        super(name, persister);
        _strict = strict;
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* onEvent:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Process event.  Support for strict transition processing is implemented
     * here, but the multiple attempt capability of the superclass is not 
     * supported.
     * 
     * When this instance is operating in strict mode, callers should catch
     * IllegalStateException's to detect when an unexpected event occurred.
     *
     * @param stateful the entity that contains the state variable
     * @param event the event attempting to cause a transition
     * @param args optional parameters to pass into the Action
     * @return the new state, which can be the same as the old state
     * @throws IllegalStateException when state has no transition defined for the event
     */
    @Override
    public State<T> onEvent(T stateful, String event, Object ... args)  
     throws IllegalStateException
    {
        // Attempt to transition to a new state given the current state
        // and an event.
        State<T> current = this.getCurrentState(stateful);

        // Fetch the transition for this event from the current state
        Transition<T> transition = this.getTransition(event, current);

        // Is a transition defined?  Note that retry exceptions are
        // never thrown because we don't support multiple attempts
        // to make a transition.
        if (transition != null) {
            
            // Debug statement looks like: 
            //  FSMName(StatefulEntity)::StateA(EventB)->StateB/ActionB
            if (_log.isDebugEnabled()) {
                
                // Collect action information.
                String newStateName = "UNKNOWN";
                String actionClass = "noop";
                try {
                    StateActionPair<T> pair = transition.getStateActionPair(stateful, event, args);
                    State<T> newState = pair.getState();
                    if (newState != null) newStateName = newState.getName();
                    Action<T> action = pair.getAction();
                    if (action != null) actionClass = action.getClass().getSimpleName();
                } catch (Exception e) {}
                
                _log.debug(getClass().getSimpleName() + " transition: " +
                           this.getName() + "(" +
                           stateful.getClass().getSimpleName() + ")::" +
                           current.getName() + "(" +
                           event + ")->" +
                           newStateName + "/" +
                           actionClass);
            }
            
            try {current = this.transition(stateful, current, event, transition, args);}
             catch (RetryException e){
                 // This should never happen, but just in case...
                 String msg = MsgUtils.getMsg("JOBS_STATE_RETRY_ERROR", e.getMessage());
                 throw new RuntimeException(msg, e);
             } 
        } 
        else {
            // Strict transition processing means we throw an 
            // exception when an unexpected event occurs. The
            // exception is unchecked to conform to the superclass's
            // method signature.
            if (_strict) 
            {
                String msg = MsgUtils.getMsg("JOBS_STATE_ILLEGAL_TRANSITION", current.getName(), event);
                throw new IllegalStateException(msg);
            }
            
            // Debug statement looks like: 
            //  FSMName(StatefulEntity)::StateA(EventB)->StateA/noop
            if (_log.isDebugEnabled())
                _log.debug("No transition defined: " +
                           this.getName() + "(" +
                           stateful.getClass().getSimpleName() + ")::" +
                           current.getName() + "(" +
                           event + ")->" +
                           current.getName() +
                           "/noop");
       }

       // Return the new current state.
       return current;
    }

    /* ---------------------------------------------------------------------- */
    /* isStrict:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Indicate whether we are running is strict mode or not.
     * @return true if in strict mode, false otherwise.
     */
    public boolean isStrict(){return _strict;}

    /* ---------------------------------------------------------------------- */
    /* setStrict:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Reset strict mode setting.
     * @param strict true to turn strict mode on, false to turn it off.
     */
    public void setStrict(boolean strict)
    {
        this._strict = strict;
    }
}
