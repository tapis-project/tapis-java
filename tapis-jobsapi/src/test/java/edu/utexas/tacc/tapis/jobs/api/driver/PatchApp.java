package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.ReqCreateApp;
import edu.utexas.tacc.tapis.apps.client.gen.model.ReqPatchApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class PatchApp 
{
    // ***************** Configuration Parameters *****************
    private static final String BASE_URL   = "https://dev.develop.tapis.io";
    private static final String userJWT = 
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIxOTVhNzU2YS1hZWRhLTQ2NjQtODFhMS1hODg1ZDZhZmJiNDMiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE5MjU4MzgyMzF9.wbyeWa6PQpROtnPWpykKc9ln2TQj04cD_uwjS40UeF5PMDJ7jd5u8GJ0JPyaH-qj9R3H9-J4H9vQGPnKQg7Wqj9_QIja9t5g5WM7Vz70TaXmu91EO3_rbJkmguXZMRFdBS0YFDYGLccO2i50NVyt3i-nVRAp3nFCn5-eB6UEoU_KEe5MiFnMmuzF6kUIGDi6Cw_26DxI_SsY-zcpjCmX0jx5cM0xqLv8XNv1RIVr8o9fKGuvGupdT0ZdTCp_MiMBPi11OE7OCYo7iwp-yglcpOMlQF8LOCJe9txzJGqcCZSGbQBi4mNLasyYn0cstVak9ToGQpEpiSdz4FvQtSKT9A";
    // ***************** End Configuration Parameters *************
    
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
        var patcher = new PatchApp();
        patcher.patch(args);
    }
    
    /** Create a tapis application from input in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void patch(String[] args) throws IOException, TapisClientException
    {
        // Get the current directory.
        String curDir = System.getProperty("user.dir");
        String reqDir = curDir + "/" + REQUEST_SUBDIR;
        
        // Check the input.
        if (args.length < 3) {
            System.out.println("Please supply 3 parameters in order:\n\n"
                               + "  - the name of a request file in directory " + reqDir + "\n"
                               + "  - the application id\n"
                               + "  - the application version");
            return;
        }
        
        // Read the file into a string.
        Path reqFile = Path.of(reqDir, args[0]);
        String reqString = Files.readString(reqFile);
        ReqPatchApp payload = TapisGsonUtils.getGson().fromJson(reqString, ReqPatchApp.class);
        
        // Get the application id and version from the command line.
        String appId = args[1];
        String appVersion = args[2];
        
        // Informational message.
        System.out.println("Processing " + reqFile.toString() + ".");
        // System.out.println(reqString);
        
        // Convert json string into an app create request.
        ReqCreateApp appReq = TapisGsonUtils.getGson().fromJson(reqString, ReqCreateApp.class);
        
        // Create the app.
        var appsClient = new AppsClient(BASE_URL, userJWT);
        appsClient.updateApp(appId, appVersion, payload);
        System.out.println("Finished processing " + reqFile.toString() + ".");
    }
}
