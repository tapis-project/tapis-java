package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** Each of the job event types have a standard description that can have more detailed information
 * appended to them.  The intent is to NOT include stack trace information in the JOB_ERROR_MESSAGE
 * description, but instead just extract the messages from a chain of exceptions and append those
 * to the description as an enumerated list.     
 * 
 * The events get added to the job_events table, which is referenced when users request a job's 
 * history.  The events are also submitted to the Notifications service for delivery to any
 * subscribers if they exist.
 * 
 * @author rcardone
 */
public enum JobEventType
{
    JOB_NEW_STATUS("The job has transitioned to a new status: "),
    JOB_INPUT_TRANSACTION_ID("A request to stage job input files has been submitted."),
    JOB_ARCHIVE_TRANSACTION_ID("A request to archive job output files has been submitted."),
    JOB_ERROR_MESSAGE("The job experienced an error.");
    
    private final String description;
    
    private JobEventType(String desc) {description = desc;}

    public String getDescription() {return description;}
}
