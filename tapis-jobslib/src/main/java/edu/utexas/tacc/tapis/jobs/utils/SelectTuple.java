package edu.utexas.tacc.tapis.jobs.utils;

public class SelectTuple {
	boolean validFlag = true;
	String selectStr = "str";
	
	public SelectTuple(boolean validFlag, String selectStr){
		this.validFlag = validFlag;
		this.selectStr = selectStr;
	}
	public String getSelectStr(){
		return selectStr;
	}
	public boolean getValidFlag() {
		return validFlag;
	}	
	
	
}


