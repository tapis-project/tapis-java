package edu.utexas.tacc.tapis.jobs.statemachine.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.model.Action;

import edu.utexas.tacc.tapis.jobs.statemachine.IJobFSMStatefulEntity;

public class NoopAction<T extends IJobFSMStatefulEntity> 
 implements Action<T> 
{
    /* ********************************************************************** */
    /*                                Constants                               */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(NoopAction.class);

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* Execute:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Events may cause the current state to transition to a new state, but
     * the consequences of that transition are handled by the onEvent caller.
     * Thus, events merely validate whether a transition is legal from the 
     * current state.
     */
    @Override
    public void execute(T stateful, 
                        String event, 
                        Object ... args) 
    {
        if (_log.isDebugEnabled()) {
            String msg = stateful.getClass().getSimpleName() +
                         " with new state " + stateful.getState() +
                         " received event " + event + " triggering action " +
                         getClass().getSimpleName() + ".";
            _log.debug(msg);
        }
    }   

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
