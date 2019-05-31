package edu.utexas.tacc.tapis.shared.exceptions.runtime;

public class AloeRuntimeException 
 extends RuntimeException 
{
	private static final long serialVersionUID = -6574079815352309369L;
	
	public AloeRuntimeException(String message) {super(message);}
	public AloeRuntimeException(String message, Throwable cause) {super(message, cause);}
}
