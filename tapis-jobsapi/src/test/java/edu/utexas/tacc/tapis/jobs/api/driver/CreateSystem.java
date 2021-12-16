package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.ReqPostSystem;

public class CreateSystem
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
        var creator = new CreateSystem();
        creator.create(args);
    }
    
    /** Create a tapis application from input in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void create(String[] args) throws IOException, TapisClientException
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
        
        // Informational message.
        System.out.println("Processing " + reqFile.toString() + ".");
        // System.out.println(reqString);
        
        // Convert json string into an app create request.
        ReqPostSystem sysReq = TapisGsonUtils.getGson().fromJson(reqString, ReqPostSystem.class);
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        
        // Create the system.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        sysClient.createSystem(sysReq);
    }
}
