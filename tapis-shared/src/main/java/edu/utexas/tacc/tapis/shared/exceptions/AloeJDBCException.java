package edu.utexas.tacc.tapis.shared.exceptions;

public class AloeJDBCException 
 extends AloeException 
{
	private static final long serialVersionUID = 8135445222246647477L;
	
	public AloeJDBCException(String message) {super(message);}
	public AloeJDBCException(String message, Throwable cause) {super(message, cause);}
}
