package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobCanceler {
	/** Run the remote termination command. */
	 void cancel() throws TapisException;
}
