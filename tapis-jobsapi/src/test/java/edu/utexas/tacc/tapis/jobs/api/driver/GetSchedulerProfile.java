package edu.utexas.tacc.tapis.jobs.api.driver;

import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerProfile;

public class GetSchedulerProfile 
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
        SchedulerProfile profile = null;
        try {profile = sysClient.getSchedulerProfile(args[0]);}
            catch (TapisClientException e) {
                if (e.getCode() == 404) System.out.println("Scheduler profile \"" + args[0] + "\" NOT FOUND.");
                  else e.printStackTrace();
                return;
            }
        System.out.println(profile.toString());
        
        // Print more info.
        if (profile.getHiddenOptions() != null && !profile.getHiddenOptions().isEmpty()) {
            var opt = profile.getHiddenOptions().get(0);
            System.out.println("Hidden value for " + opt.name() + " = " +
                               SystemsClient.getSchedulerHiddenOptionValue(opt));
        }
    }
}
