package edu.utexas.tacc.tapis.jobs.api.driver;

import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;

public class DeleteSchedulerProfile 
{
    public static void main(String[] args) throws Exception
    {
        // Check the input.
        if (args.length < 2) {
            System.out.println("Supply the name of a scheduler profile and a user profile name.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        
        // Create the app.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        int deleted = 0;
        try {deleted = sysClient.deleteSchedulerProfile(args[0]);}
            catch (TapisClientException e) {
                if (e.getCode() == 404) System.out.println("Scheduler profile \"" + args[0] + "\" NOT FOUND.");
                  else e.printStackTrace();
                return;
            }
        if (deleted > 0) System.out.println("Deleted profile " + args[0]);
          else System.out.println("Profiles deleted: " + deleted);
    }
}
