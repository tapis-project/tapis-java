package edu.utexas.tacc.tapis.jobs.reader;

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

final class QueueReaderParameters
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(QueueReaderParameters.class);
    
    // Length limits.
    private static final int MAX_WORKER_NAME_LEN = 16;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    @Option(name = "-n", required = true, aliases = {"-name"}, 
            metaVar = "<name>", usage = "User chosen name for the reader instance")
    public String name;

    // Used by event reader; reassigned by all other queue readers.
    @Option(name = "-b", required = false, aliases = {"-bindingkey"}, 
            metaVar = "<binding key>", usage = "Key to bind queue or topic to exchange")
    public String bindingKey = JobQueueManagerNames.DEFAULT_BINDING_KEY;;
    
    @Option(name = "-help", aliases = {"--help"}, 
            usage = "display help information")
    public boolean help;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public QueueReaderParameters(String[] args) 
     throws JobException 
    {
        // Get the input parameters.
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
            writer.write("EventTopicReader [options...]\n");
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
         String s = "\nEventTopicReader reads a tenant's event queue with an optional binding key.";
         System.out.println(s);
         System.out.println("\nEventTopicReader [options...]\n");
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
      // Regex that allows alphanumerics plus [_.-] in string parameters.
      Pattern pattern = Pattern.compile("^[\\p{IsAlphabetic}\\p{IsDigit}_\\.\\-]+$");
      
      // Truncate name if necessary.
      if (name.length() > MAX_WORKER_NAME_LEN) name = name.substring(0, MAX_WORKER_NAME_LEN -1);
      
      // Allow alphanumerics plus [_.-].
      if (!pattern.matcher(name).matches()) {
          String msg = MsgUtils.getMsg("JOBS_WORKER_INVALID_CHAR", "name", name);
          _log.error(msg);
          throw new JobInputException(msg);
      }
    }
}