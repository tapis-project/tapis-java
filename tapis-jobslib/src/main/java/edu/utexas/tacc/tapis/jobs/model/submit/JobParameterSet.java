package edu.utexas.tacc.tapis.jobs.model.submit;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.shared.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;

/** This is the java model for the parameterSet JSON value defined in 
 * SubmitJobRequest.json. 
 * 
 * @author rcardone
 */
public class JobParameterSet 
{
    private List<JobArgSpec>     appArgs;
    private List<JobArgSpec>     containerArgs;
    private List<JobArgSpec>     schedulerOptions;
    private List<KeyValuePair>   envVariables;
    private IncludeExcludeFilter archiveFilter;
    
    // Constructors.
    public JobParameterSet() {this(true);}
    public JobParameterSet(boolean initialize) {if (initialize) initAll();}
    
    // A simple way to make sure all lists and other fields are non-null.
    public void initAll()
    {
        // Don't stomp on existing data.
        if (appArgs == null) appArgs = new ArrayList<JobArgSpec>();
        if (containerArgs == null) containerArgs = new ArrayList<JobArgSpec>();
        if (schedulerOptions == null) schedulerOptions = new ArrayList<JobArgSpec>();
        if (envVariables == null) envVariables = new ArrayList<KeyValuePair>();
        if (archiveFilter == null) archiveFilter = new IncludeExcludeFilter();
        archiveFilter.initAll();
    }
    
    public List<JobArgSpec> getAppArgs() {
        return appArgs;
    }
    public void setAppArgs(List<JobArgSpec> appArgs) {
        this.appArgs = appArgs;
    }
    public List<JobArgSpec> getContainerArgs() {
        return containerArgs;
    }
    public void setContainerArgs(List<JobArgSpec> containerArgs) {
        this.containerArgs = containerArgs;
    }
    public List<JobArgSpec> getSchedulerOptions() {
        return schedulerOptions;
    }
    public void setSchedulerOptions(List<JobArgSpec> schedulerOptions) {
        this.schedulerOptions = schedulerOptions;
    }
    public List<KeyValuePair> getEnvVariables() {
        return envVariables;
    }
    public void setEnvVariables(List<KeyValuePair> envVariables) {
        this.envVariables = envVariables;
    }
    public IncludeExcludeFilter getArchiveFilter() {
        return archiveFilter;
    }
    public void setArchiveFilter(IncludeExcludeFilter archiveFilter) {
        this.archiveFilter = archiveFilter;
    }
}
