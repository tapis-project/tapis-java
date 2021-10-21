package edu.utexas.tacc.tapis.jobs.model.submit;

import java.util.List;

public class JobFileInputArray 
{
    private String       name;
    private String       description;
    private List<String> sourceUrls;
    private String       targetDir;
    
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
    public List<String> getSourceUrls() {
        return sourceUrls;
    }
    public void setSourceUrls(List<String> sourceUrls) {
        this.sourceUrls = sourceUrls;
    }
    public String getTargetDir() {
        return targetDir;
    }
    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }
}
