package edu.utexas.tacc.tapis.jobs.exceptions;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@SuppressWarnings("serial")
public class JobException extends TapisException {

	public JobException(String message)
	{
		super(message);
	}

	public JobException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
