package edu.utexas.tacc.tapis.jobs.worker;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.regex.Pattern;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobInputException;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobWorkerParameters 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobWorkerParameters.class);
  
  // Ranges and lengths.
  public static final int MIN_WORKERS = 1;
  public static final int MAX_WORKERS = 255;
  public static final int MAX_WORKER_NAME_LEN = 16;
  public static final int MAX_TENANT_LEN = 64;
  public static final int MAX_QUEUE_NAME_LEN = 255;
  public static final int MAX_USER_LEN = 32;
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  @Option(name = "-n", required = true, aliases = {"-name"}, 
      metaVar = "<name>", usage = "worker name for human consumption")
  public String name;
  
  @Option(name = "-q", required = true, aliases = {"-queue"}, 
      metaVar = "<queue name>", usage = "work queue name")
  public String queueName;

  @Option(name = "-w", required = true, aliases = {"-workers",}, 
      metaVar = "<# of workers>", usage = "the number of queue reading threads")
  public int numWorkers;

  @Option(name = "-allowtest", required = false,
      usage = "allow parameters used for testing (test* parms)")
  public boolean allowTestParms;

  @Option(name = "-testuser", required = false, depends = {"-allowtest"}, 
      metaVar = "<user name>", usage = "user")
  public String testUser;

  @Option(name = "-help", aliases = {"--help"}, 
      usage = "display help information")
  public boolean help;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  public JobWorkerParameters(String[] args) 
   throws JobException
  {
    initializeParms(args);
    validateParms();
  }
  
  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* initializeParms:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Parse the input arguments. */
  private void initializeParms(String[] args)
      throws JobException
  {
    // Get a command line parser to verify input.
    CmdLineParser parser = new CmdLineParser(this);
    parser.getProperties().withUsageWidth(120);
    
    try {
       // Parse the arguments.
       parser.parseArgument(args);
      }
     catch (CmdLineException e)
      {
       if (!help)
         {
          // Create message buffer of sufficient size.
          final int initialCapacity = 1024;
          StringWriter writer = new StringWriter(initialCapacity);
          
          // Write parser error message.
          writer.write("\n******* Input Parameter Error *******\n");
          writer.write(e.getMessage());
          writer.write("\n\n");
          
          // Write usage information--unfortunately we need an output stream.
          writer.write("JobWorker [options...]\n");
          ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
          parser.printUsage(ostream);
          try {writer.write(ostream.toString("UTF-8"));}
            catch (Exception e1) {}
          writer.write("\n");
          
          // Throw exception.
          throw new JobException(writer.toString());
         }
      }
    
    // Display help and exit program.
    if (help)
      {
       String s = "\nJobWorkers process tenant worker queues.";
       System.out.println(s);
       System.out.println("\nJobWorker [options...]\n");
       parser.printUsage(System.out);
       System.exit(0);
      }
  }
  
  /* ---------------------------------------------------------------------- */
  /* validateParms:                                                         */
  /* ---------------------------------------------------------------------- */
  /** Check the semantic integrity of the input parameters.
   * 
   * @throws JobException
   */
  private void validateParms()
   throws JobException
  {
    // --- The number of workers must be in range.
    if ((numWorkers < MIN_WORKERS) || (numWorkers > MAX_WORKERS)) {
      String msg = MsgUtils.getMsg("JOBS_WORKER_NUMBER_WORKERS", numWorkers, MIN_WORKERS, MAX_WORKERS);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    
    // Regex that allows alphanumerics plus [_.-] in string parameters.
    Pattern pattern = Pattern.compile("^[\\p{IsAlphabetic}\\p{IsDigit}_\\.\\-]+$");
    
    // --- Worker name checks.
    if (name.length() > MAX_WORKER_NAME_LEN) {
      String msg = MsgUtils.getMsg("JOBS_WORKER_PARM_LENGTH", "name", MAX_WORKER_NAME_LEN);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    // Allow alphanumerics plus [_.-].
    if (!pattern.matcher(name).matches()) {
      String msg = MsgUtils.getMsg("JOBS_WORKER_INVALID_CHAR", "name", name);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    
    // Queue name checks.
    if (queueName.length() > MAX_QUEUE_NAME_LEN) {
      String msg = MsgUtils.getMsg("JOBS_WORKER_PARM_LENGTH", "queue", MAX_QUEUE_NAME_LEN);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    // Allow alphanumerics plus [_.-].
    if (!pattern.matcher(queueName).matches()) {
      String msg = MsgUtils.getMsg("JOBS_WORKER_INVALID_CHAR", "queue", queueName);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    // Make sure the queue name starts with the required prefix.  This also
    // confirms that the queuename and tenantId are compatible.
    String prefix = JobQueueManagerNames.TAPIS_JOBQ_PREFIX + JobQueueManagerNames.SUBMIT_PART;
    if (!queueName.startsWith(prefix)) {
      String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_NAME_PREFIX", prefix);
      _log.error(msg);
      throw new JobInputException(msg);
    }
    // Make sure there's something after the prefix.
    if (queueName.length() == prefix.length()) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_NAME_NO_SUFFIX", prefix);
        _log.error(msg);
        throw new JobInputException(msg);
    }
    // There should be no periods in the suffix--it's the unsegmented queue name.
    if (queueName.indexOf('.', prefix.length()) >= 0) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_NAME_SUFFIX", prefix, queueName);
        _log.error(msg);
        throw new JobInputException(msg);
    }
    
    // The user can only specify test* parameters when the -allowtest parameter was specified.
    if (testUser != null && (testUser.length() > MAX_USER_LEN)) {
        String msg = MsgUtils.getMsg("JOBS_WORKER_PARM_LENGTH", "testUser", MAX_USER_LEN);
        _log.error(msg);
        throw new JobInputException(msg);
    }
  }
}
