package edu.utexas.tacc.tapis.jobs.exceptions.runtime;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;

public class JobAsyncCmdException extends TapisRuntimeException 
{
    private static final long serialVersionUID = 1504196475458839136L;

    public JobAsyncCmdException(String message)
	{
		super(message);
	}

	public JobAsyncCmdException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
