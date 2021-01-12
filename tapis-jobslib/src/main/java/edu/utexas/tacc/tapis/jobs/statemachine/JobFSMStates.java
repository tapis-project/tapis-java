package edu.utexas.tacc.tapis.jobs.statemachine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.impl.StateImpl;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

/** This class defines all possible states (i.e., statuses) that a job can be in
 * and all valid state transitions.
 * 
 * JobFSMTest tests every possible state transition.  If the set of states changes,
 * then that test program should be changed accordingly.
 * 
 * @author rcardone
 *
 */
public final class JobFSMStates
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // All states known to the state machine.
    public static final State<JobFSMStatefulEntity> Pending = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.PENDING.name());
    public static final State<JobFSMStatefulEntity> ProcessingInputs = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.PROCESSING_INPUTS.name());
    public static final State<JobFSMStatefulEntity> StagingInputs = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.STAGING_INPUTS.name());

    public static final State<JobFSMStatefulEntity> StagingJob = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.STAGING_JOB.name());
    public static final State<JobFSMStatefulEntity> SubmittingJob = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.SUBMITTING_JOB.name());
    public static final State<JobFSMStatefulEntity> Queued = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.QUEUED.name());
    public static final State<JobFSMStatefulEntity> Running = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.RUNNING.name());
    
    public static final State<JobFSMStatefulEntity> Archiving = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.ARCHIVING.name());
    
    public static final State<JobFSMStatefulEntity> Blocked = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.BLOCKED.name());
    public static final State<JobFSMStatefulEntity> Paused = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.PAUSED.name());
    
    public static final State<JobFSMStatefulEntity> Finished = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.FINISHED.name());
    public static final State<JobFSMStatefulEntity> Cancelled = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.CANCELLED.name());
    public static final State<JobFSMStatefulEntity> Failed = 
            new StateImpl<JobFSMStatefulEntity>(JobStatusType.FAILED.name());
    
    // Create the unmodifiable list of all above defined states.
    private static final List<State<JobFSMStatefulEntity>> _states = createStateList(); 
    
    /* ********************************************************************** */
    /*                             Initializers                               */
    /* ********************************************************************** */
    static {
        // Create all state transitions.
        initializeTransitions();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getStates:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Return the statically defined unmodifiable list of states.
     * 
     * @return the unmodifiable list of all states
     */
    public static List<State<JobFSMStatefulEntity>> getStates(){return _states;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createStateList:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Create an unmodifiable list of all states known to the state machine.
     * 
     * @return a new unmodifiable list of all states
     */
    private static List<State<JobFSMStatefulEntity>> createStateList()
    {
        // Don't forget to update the initial capacity to match the number of states.
        // Also, add new state to JobFSMUtils methods that switch on state.
        List<State<JobFSMStatefulEntity>> states = new ArrayList<State<JobFSMStatefulEntity>>(16);
        states.add(Pending);
        states.add(ProcessingInputs);
        states.add(StagingInputs);
        states.add(StagingJob);
        states.add(SubmittingJob);
        states.add(Queued);
        states.add(Running);
        states.add(Archiving);
        states.add(Blocked);
        states.add(Paused);
        states.add(Finished);
        states.add(Cancelled);
        states.add(Failed);
        return Collections.unmodifiableList(states);
    }
    
    /* ---------------------------------------------------------------------- */
    /* initializeTransitions:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Define the transitions for all states.  All transitions are deterministic.
     */
    private static void initializeTransitions()
    {
        /* Deterministic Transitions */
        //
        // When transitions are defined with an action, the result is:
        //
        //      stateA(eventA) -> stateB/actionA
        //
        // and without an action:
        //
        //      stateA(eventA) -> stateB/noop

        /* ================================================================== */
        /* Forward Processing Transitions                                     */
        /* ================================================================== */
        // Since we only use the state machine to validate that a transition is
        // legal, we don't need to specify actions on transitions.  When used
        // with the JobFSM, exceptions are thrown on undefined transitions.
        
        // ------ From Pending (initial state)
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_PENDING.name(), JobFSMStates.Pending);
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_PROCESSING_INPUTS.name(), JobFSMStates.ProcessingInputs);
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Pending.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
               
        // ------ From ProcessingInputs
        // If there are no inputs to stage, we transition directly to staging job.
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_PROCESSING_INPUTS.name(), JobFSMStates.ProcessingInputs);
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_STAGING_INPUTS.name(), JobFSMStates.StagingInputs);
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.ProcessingInputs.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        
        // ------ From StagingInputs
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_STAGING_INPUTS.name(), JobFSMStates.StagingInputs);
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_STAGING_JOB.name(), JobFSMStates.StagingJob);
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.StagingInputs.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        
        // ------ From StagingJob
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_STAGING_JOB.name(), JobFSMStates.StagingJob);
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_SUBMITTING_JOB.name(), JobFSMStates.SubmittingJob);
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.StagingJob.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
                       
        // ------ From Submitting
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_SUBMITTING_JOB.name(), JobFSMStates.SubmittingJob);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_QUEUED.name(), JobFSMStates.Queued);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_RUNNING.name(), JobFSMStates.Running);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.SubmittingJob.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        
        // ------ From Queued
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_QUEUED.name(), JobFSMStates.Queued);
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_RUNNING.name(), JobFSMStates.Running);
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Queued.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        
        // ------ From Running
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_RUNNING.name(), JobFSMStates.Running);
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_ARCHIVING.name(), JobFSMStates.Archiving);
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Running.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
             
        // ------ From Archiving
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_ARCHIVING.name(), JobFSMStates.Archiving);
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_FINISHED.name(), JobFSMStates.Finished);     
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_BLOCKED.name(), JobFSMStates.Blocked);
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Archiving.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);

        // ------ From Blocked
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_PENDING.name(), JobFSMStates.Pending);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_PROCESSING_INPUTS.name(), JobFSMStates.ProcessingInputs);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_STAGING_INPUTS.name(), JobFSMStates.StagingInputs);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_STAGING_JOB.name(), JobFSMStates.StagingJob);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_SUBMITTING_JOB.name(), JobFSMStates.SubmittingJob);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_QUEUED.name(), JobFSMStates.Queued);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_RUNNING.name(), JobFSMStates.Running);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_ARCHIVING.name(), JobFSMStates.Archiving);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_PAUSED.name(), JobFSMStates.Paused);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        JobFSMStates.Blocked.addTransition(JobFSMEvents.TO_FINISHED.name(), JobFSMStates.Finished);

        // ------ From Paused
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_PENDING.name(), JobFSMStates.Pending);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_PROCESSING_INPUTS.name(), JobFSMStates.ProcessingInputs);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_STAGING_INPUTS.name(), JobFSMStates.StagingInputs);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_STAGING_JOB.name(), JobFSMStates.StagingJob);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_SUBMITTING_JOB.name(), JobFSMStates.SubmittingJob);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_QUEUED.name(), JobFSMStates.Queued);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_RUNNING.name(), JobFSMStates.Running);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_ARCHIVING.name(), JobFSMStates.Archiving);     
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_FAILED.name(), JobFSMStates.Failed);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_CANCELLED.name(), JobFSMStates.Cancelled);
        JobFSMStates.Paused.addTransition(JobFSMEvents.TO_FINISHED.name(), JobFSMStates.Finished);
               
        // ------ From Finished, Cancelled, Failed
        // These are terminal states.
    }
}
