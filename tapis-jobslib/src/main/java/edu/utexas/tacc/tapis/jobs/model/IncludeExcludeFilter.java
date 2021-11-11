package edu.utexas.tacc.tapis.jobs.model;

import java.util.ArrayList;
import java.util.List;

public class IncludeExcludeFilter 
{
    private List<String> includes;
    private List<String> excludes;
    private Boolean      includeLaunchFiles;
    
    public IncludeExcludeFilter() {this(true);}
    public IncludeExcludeFilter(boolean init) {if (init) initAll();}
    
    // A simple way to make sure all fields are non-null.
    public void initAll()
    {
        if (includes == null) includes = new ArrayList<String>();
        if (excludes == null) excludes = new ArrayList<String>();
    }
    
    public List<String> getIncludes() {
        return includes;
    }
    public void setIncludes(List<String> includes) {
        this.includes = includes;
    }
    public List<String> getExcludes() {
        return excludes;
    }
    public void setExcludes(List<String> excludes) {
        this.excludes = excludes;
    }
    public Boolean getIncludeLaunchFiles() {
        return includeLaunchFiles;
    }
    public void setIncludeLaunchFiles(Boolean includeLaunchFiles) {
        this.includeLaunchFiles = includeLaunchFiles;
    }
}
