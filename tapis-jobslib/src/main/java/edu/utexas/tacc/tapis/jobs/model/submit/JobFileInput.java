package edu.utexas.tacc.tapis.jobs.model.submit;

public class JobFileInput 
{
    private String  name;
    private String  description;
    private Boolean autoMountLocal;
    private String  sourceUrl;
    private String  targetPath;
    
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
    public Boolean getAutoMountLocal() {
        return autoMountLocal;
    }
    public void setAutoMountLocal(Boolean autoMountLocal) {
        this.autoMountLocal = autoMountLocal;
    }
    public String getSourceUrl() {
        return sourceUrl;
    }
    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }
    public String getTargetPath() {
        return targetPath;
    }
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }
}
