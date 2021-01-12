package edu.utexas.tacc.tapis.jobs.statemachine;

import org.statefulj.persistence.annotations.State;

/** Not much going on here--we just need a place to store state
 * so that transitions have a current state to read and a place
 * to write a new state.
 * 
 * @author rcardone
 */
public class JobFSMStatefulEntity 
 implements IJobFSMStatefulEntity
{
    @State
    String state;   // Memory Persister requires a String
    
    @Override
    public String getState(){return state;}
}
