package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisJSONException 
 extends TapisException 
{
  private static final long serialVersionUID = -8939731590644378685L;
  
  public TapisJSONException(String message) {super(message);}
	public TapisJSONException(String message, Throwable cause) {super(message, cause);}
}
