package edu.utexas.tacc.tapis.shared.exceptions.runtime;

public class TapisRuntimeException 
 extends RuntimeException 
{
	private static final long serialVersionUID = -6574079815352309369L;
	
	public TapisRuntimeException(String message) {super(message);}
	public TapisRuntimeException(String message, Throwable cause) {super(message, cause);}
}
