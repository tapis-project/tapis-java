package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.ReqCreateSchedulerProfile;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerHiddenOptionEnum;

public class CreateSchedulerProfile 
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
        var creator = new CreateSchedulerProfile();
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
        
        // Convert json string into a profile create request.
        var gsonBuilder = TapisGsonUtils.getGsonBuilder(false);
        Type HIDDEN_OPTION = new TypeToken<SchedulerHiddenOptionEnum>(){}.getType();
        gsonBuilder.registerTypeAdapter(HIDDEN_OPTION, new HiddenOptionAdapter());
        var gson = gsonBuilder.create();
        ReqCreateSchedulerProfile profileReq = gson.fromJson(reqString, ReqCreateSchedulerProfile.class);
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        
        // Create the profile.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        sysClient.createSchedulerProfile(profileReq);
        System.out.println("Finished processing " + reqFile.toString() + ".");
    }
    
    // Deserialize hiddenOption enums so they can be loaded into the profile create request.
    private static final class HiddenOptionAdapter
     implements JsonDeserializer<SchedulerHiddenOptionEnum>
    {
        @Override
        public SchedulerHiddenOptionEnum deserialize(JsonElement json, Type typeOf, JsonDeserializationContext arg2)
                throws JsonParseException 
        {
            return SchedulerHiddenOptionEnum.valueOf(json.getAsString());
        }
    }
}
