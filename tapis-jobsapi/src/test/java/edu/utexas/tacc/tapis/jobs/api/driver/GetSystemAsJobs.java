package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;

public class GetSystemAsJobs 
{
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var getSystem = new GetSystemAsJobs();
        getSystem.get(args);
    }
    
    /** Create a tapis application from input in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void get(String[] args) throws IOException, TapisClientException
    {
        // Check the input.
        if (args.length < 2) {
            System.out.println("Supply the system name and the profile name.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        
        // Get the system.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        sysClient.addDefaultHeader("X-TAPIS-USER", "cicsvc");
        sysClient.addDefaultHeader("X-TAPIS-TENANT", "tacc");
//        sysClient.addDefaultHeader("X-TAPIS-USER", "testuser2");
//        sysClient.addDefaultHeader("X-TAPIS-TENANT", "dev");
        Boolean returnCreds = Boolean.TRUE;
        Boolean checkExec   = Boolean.FALSE;
        AuthnMethod  defaultAuthMethod = null;
        var sys = sysClient.getSystem(args[0], returnCreds, defaultAuthMethod, checkExec);
        System.out.println(sys.toString());
    }
}
