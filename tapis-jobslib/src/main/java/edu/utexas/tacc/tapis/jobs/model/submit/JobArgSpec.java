package edu.utexas.tacc.tapis.jobs.model.submit;

import edu.utexas.tacc.tapis.jobs.model.Job;

public class JobArgSpec 
{
    private String  name;
    private String  description;
    private Boolean include;
    private String  arg;
    private String  notes;
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public Boolean getInclude() {
        return include;
    }
    public void setInclude(Boolean include) {
        this.include = include;
    }
    public String getArg() {
        return arg;
    }
    public void setArg(String arg) {
        this.arg = arg;
    }
    public String getNotes() {
        return notes;
    }
    public void setNotes(String notes) {
        this.notes = notes;
    }
}
