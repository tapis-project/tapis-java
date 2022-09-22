package edu.utexas.tacc.tapis.jobs.utils;

public class JobOutputInfo {
	String systemId;
	String systemDir;
	String systemUrl;
	boolean isArchiveSystem;
	
	JobOutputInfo(String systemId, String systemDir, String systemUrl, boolean isArchiveSystem){
		this.systemId = systemId;
		this.systemDir = systemDir;
		this.systemUrl = systemUrl;
		this.isArchiveSystem = isArchiveSystem;
	}
	public String getSystemId() {
		return systemId;
	}
	public String getSystemDir() {
		return systemDir;
	}
	public String getSystemUrl() {
		return systemUrl;
	}
    public boolean isArchiveSystem() {
		return isArchiveSystem;
	}
    
}
