package edu.utexas.tacc.aloe.shared.exceptions;

public class AloeException 
 extends Exception 
{
	private static final long serialVersionUID = -2996383526058549742L;

	// AloeUtils.aloeify() requires that all subclasses implement constructors
	// with these exact signatures or guarantee that aloeify() is never called 
	// with subclasses that don't implement these two constructors.  See 
	// JobUtils.aloeify() for an example of how to implement non-conforming
	// exceptions that still can be aloeified.
	public AloeException(String message) {super(message);}
	public AloeException(String message, Throwable cause) {super(message, cause);}
}
