package edu.utexas.tacc.tapis.jobs.utils;

public class JobOutputInfo {
	String systemId;
	String systemDir;
	String systemUrl;
	
	JobOutputInfo(String systemId, String systemDir, String systemUrl){
		this.systemId = systemId;
		this.systemDir = systemDir;
		this.systemUrl = systemUrl;
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

}
