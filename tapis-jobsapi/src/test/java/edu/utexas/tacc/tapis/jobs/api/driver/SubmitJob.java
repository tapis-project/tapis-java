package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.client.JobsClient;
import edu.utexas.tacc.tapis.jobs.client.gen.model.Job;
import edu.utexas.tacc.tapis.jobs.client.gen.model.ReqSubmitJob;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This test submits a job using previously created systems, apps and job definitions.
 * The command format requires a job request file name and, optionally, the base url for 
 * the jobs service.
 * 
 *      java SubmitJob <job submission request file> [jobs service base url]
 * 
 * If the second parameter is not provided, the address of the job service running on 
 * the local host is assumed.
 * 
 * @author rcardone
 */
public class SubmitJob 
{
    // Subdirectory relative to current directory where request files are kept.
    private static final String REQUEST_SUBDIR = "requests";
    
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var submitter = new SubmitJob();
        submitter.submit(args);
    }
    
    /** Submit a job request defined in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void submit(String[] args) throws IOException, TapisClientException
    {
        // Get the current directory.
        String curDir = System.getProperty("user.dir");
        String reqDir = curDir + "/" + REQUEST_SUBDIR;
        
        // Check the input.
        if (args.length < 2) {
            System.out.println("Supply the name of a request file in directory " + reqDir + " and the profile name.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read the file into a string.
        Path reqFile = Path.of(reqDir, args[0]);
        String reqString = Files.readString(reqFile);
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        String url = props.getProperty("BASE_URL");
        if (!url.endsWith("/v3")) url += "/v3";
        
        // Informational message.
        System.out.println("Processing " + reqFile.toString() + ".");
        System.out.println("Contacting Jobs Service at " + url + ".");
        // System.out.println(reqString);
        
        // Convert json string into a job submission request.
        ReqSubmitJob submitReq = TapisGsonUtils.getGson().fromJson(reqString, ReqSubmitJob.class);
        
        // Create a job client.
        var jobClient = new JobsClient(url, props.getProperty("USER_JWT"));
        Job job = jobClient.submitJob(submitReq);
        System.out.println(TapisGsonUtils.getGson(true).toJson(job));
        
        System.out.println();
        System.out.println("-----------------");
        System.out.println(job.getParameterSet());
    }
}
