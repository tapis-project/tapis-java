package edu.utexas.tacc.tapis.jobs.model.submit;

import java.util.List;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppFileInputArray;
import edu.utexas.tacc.tapis.apps.client.gen.model.FileInputModeEnum;

public class JobFileInputArray 
{
    private String       name;
    private String       description;
    private List<String> sourceUrls;
    private String       targetDir;
    private boolean      optional = false;
    
    public boolean emptySourceUrls()
    {return sourceUrls == null || sourceUrls.isEmpty();}
    
    public boolean equalSourceUrls​(List<String> urls) {
        // We don't distinguish between null and empty when
        // checking for equality.
        if ((sourceUrls == null || sourceUrls.isEmpty()) && 
            (urls == null || urls.isEmpty())) return true;
        
        // XOR
        if ((sourceUrls == null || sourceUrls.isEmpty()) ^ 
            (urls == null || urls.isEmpty())) return false;
        
        // If we get here, neither list is null (or empty)
        // since TT, TF and FT cases have been eliminated,
        // leaving only FF (i.e., neither list is null (or 
        // empty).  So they both have non-zero sizes.
        if (sourceUrls.size() != urls.size()) return false;
        for (int i = 0; i < sourceUrls.size(); i++)
            if (!sourceUrls.get(i).equals(urls.get(i))) return false;
            
        // The two non-empty lists are the same size and
        // they contain the same strings in the same order.
        return true;
    }
    
    // Import an app input into a request input.
    public static JobFileInputArray importAppInputArray(AppFileInputArray appInput)
    {
        var reqInput = new JobFileInputArray();
        reqInput.setName(appInput.getName());
        reqInput.setDescription(appInput.getDescription());
        reqInput.setSourceUrls(appInput.getSourceUrls());
        reqInput.setTargetDir(appInput.getTargetDir());
        if (appInput.getInputMode() == null ||
            appInput.getInputMode() == FileInputModeEnum.OPTIONAL)
            reqInput.setOptional(true);
        return reqInput;
    }
    
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
    public boolean isOptional() {
        return optional;
    }
    public void setOptional(boolean optional) {
        this.optional = optional;
    }
}
