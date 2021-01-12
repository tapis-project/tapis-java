package edu.utexas.tacc.tapis.jobs.statemachine;

import org.statefulj.fsm.model.State;
import org.statefulj.persistence.memory.MemoryPersisterImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/** This class tests all possible state transitions for every state, resulting
 * in a grand total of 169 (13 * 13) transitions attempts.  For transitions that 
 * are expected to succeed, their new state is checked.  For transitions that
 * are expected to fail because they are not defined, the expected exception
 * is caught.
 * 
 * Whenever a new state is added a new test method should be implemented and 
 * all existing test methods should be modified to include the new state as
 * a transition target.
 * 
 * @author rcardone
 *
 */
@Test(groups={"unit"})
public class JobFSMTest
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // In-memory persister for state machine.
    private MemoryPersisterImpl<JobFSMStatefulEntity> _persister;
    
    // State machine.
    private JobFSM<JobFSMStatefulEntity> _jsm;
    
    // Stateful entity object that holds state variable.
    private JobFSMStatefulEntity _entity;
    
    // Flag used to confirm that an invalid transistion was attempted.
    private boolean _exceptionThrown;
    
    /* ********************************************************************** */
    /*                            Set Up / Tear Down                          */
    /* ********************************************************************** */    
    /* ---------------------------------------------------------------------- */
    /* setup:                                                                 */
    /* ---------------------------------------------------------------------- */
    @BeforeSuite
    private void setup()
    {
        // Create the memory-based persister.
        _persister = new MemoryPersisterImpl<JobFSMStatefulEntity>(
                                            JobFSMStates.getStates(),   
                                            JobFSMStates.Pending);  // Start State        

        // Create the Jobs FSM.
        boolean strict = true;
        _jsm = new JobFSM<JobFSMStatefulEntity>("Test JobFSM", _persister, strict);
        
        // Create the entity
        _entity = new JobFSMStatefulEntity();
    }
    
    /* ---------------------------------------------------------------------- */
    /* setupMethod:                                                           */
    /* ---------------------------------------------------------------------- */
    @BeforeMethod
    private void setupMethod()
    {
        _exceptionThrown = false;
    }
    
    /* ********************************************************************** */
    /*                              Test Methods                              */
    /* ********************************************************************** */    
    /* ---------------------------------------------------------------------- */
    /* fromPending:                                                           */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromPending()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Pending.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.ProcessingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Pending);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromProcessingInputs:                                                  */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromProcessingInputs()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.ProcessingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.ProcessingInputs);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromStagingInputs:                                                     */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromStagingInputs()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.StagingInputs);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromStagingJob:                                                        */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromStagingJob()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.SubmittingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.StagingJob);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromSubmittingJob:                                                     */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromSubmittingJob()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.SubmittingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Queued.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Running.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.SubmittingJob);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromQueued:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromQueued()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Queued.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Running.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Queued);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
   }

    /* ---------------------------------------------------------------------- */
    /* fromRunning:                                                           */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromRunning()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Running);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Running.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Running);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Archiving.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Running);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Running);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Running);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Running);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Running);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromArchiving:                                                         */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromArchiving()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Archiving.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Finished.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Blocked.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");
        
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Archiving);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromBlocked:                                                           */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromBlocked()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Pending.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.ProcessingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.SubmittingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Queued.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Running.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Archiving.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Paused.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Finished.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");

        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Blocked);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_BLOCKED.name()); 
    }
   
    /* ---------------------------------------------------------------------- */
    /* fromPaused:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromPaused()
    {   
        // ----- Legal transitions.
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        State<JobFSMStatefulEntity> state = _jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Pending.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.ProcessingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingInputs.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.StagingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());
        Assert.assertEquals(state.getName(), JobFSMStates.SubmittingJob.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Queued.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Running.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Archiving.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Failed.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Finished.getName(), 
                            "Transitioned to the wrong state!");
        
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        state = _jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());
        Assert.assertEquals(state.getName(), JobFSMStates.Cancelled.getName(), 
                            "Transitioned to the wrong state!");

        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Paused);
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_BLOCKED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PAUSED.name()); 
    }
   
    /* ---------------------------------------------------------------------- */
    /* fromFailed:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromFailed()
    {   
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Failed);

        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_BLOCKED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PAUSED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FAILED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_CANCELLED.name()); 
    }

    /* ---------------------------------------------------------------------- */
    /* fromFinished:                                                          */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromFinished()
    {   
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Finished);

        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_BLOCKED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PAUSED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FAILED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_CANCELLED.name()); 
    }
    
    /* ---------------------------------------------------------------------- */
    /* fromCancelled:                                                         */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void fromCancelled()
    {   
        // ----- Illegal transitions.
        // Should be the last time we reset this.
        _persister.setCurrent(_entity, JobFSMStates.Cancelled);

        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PENDING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PENDING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PROCESSING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PROCESSING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_INPUTS.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_INPUTS.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_STAGING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_STAGING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_SUBMITTING_JOB.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_SUBMITTING_JOB.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_QUEUED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_QUEUED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_RUNNING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_RUNNING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_ARCHIVING.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_ARCHIVING.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_BLOCKED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_BLOCKED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_PAUSED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_PAUSED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FAILED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FAILED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_FINISHED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_FINISHED.name()); 
        
        _exceptionThrown = false;
        try {_jsm.onEvent(_entity, JobFSMEvents.TO_CANCELLED.name());}
        catch (IllegalStateException e){_exceptionThrown = true;}
        Assert.assertTrue(_exceptionThrown, "Illegal transition " + JobFSMEvents.TO_CANCELLED.name()); 
    }

}
