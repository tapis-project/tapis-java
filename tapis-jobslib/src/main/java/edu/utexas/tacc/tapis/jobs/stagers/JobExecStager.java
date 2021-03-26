package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobExecStager 
{
    /** Stage job assets. */
    void stageJob() throws TapisException;
}
