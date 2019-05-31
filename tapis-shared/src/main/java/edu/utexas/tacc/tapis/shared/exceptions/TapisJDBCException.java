package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisJDBCException 
 extends TapisException 
{
	private static final long serialVersionUID = 8135445222246647477L;
	
	public TapisJDBCException(String message) {super(message);}
	public TapisJDBCException(String message, Throwable cause) {super(message, cause);}
}
