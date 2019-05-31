package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisUUIDException 
 extends TapisException 
{
	private static final long serialVersionUID = 8135445222246647477L;
	
	public TapisUUIDException(String message) {super(message);}
	public TapisUUIDException(String message, Throwable cause) {super(message, cause);}
}
