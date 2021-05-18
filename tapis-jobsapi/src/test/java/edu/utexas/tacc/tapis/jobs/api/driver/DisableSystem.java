package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;

public class DisableSystem 
{
    // ***************** Configuration Parameters *****************
    private static final String BASE_URL   = "https://dev.develop.tapis.io";
    private static final String userJWT = 
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIxOTVhNzU2YS1hZWRhLTQ2NjQtODFhMS1hODg1ZDZhZmJiNDMiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE5MjU4MzgyMzF9.wbyeWa6PQpROtnPWpykKc9ln2TQj04cD_uwjS40UeF5PMDJ7jd5u8GJ0JPyaH-qj9R3H9-J4H9vQGPnKQg7Wqj9_QIja9t5g5WM7Vz70TaXmu91EO3_rbJkmguXZMRFdBS0YFDYGLccO2i50NVyt3i-nVRAp3nFCn5-eB6UEoU_KEe5MiFnMmuzF6kUIGDi6Cw_26DxI_SsY-zcpjCmX0jx5cM0xqLv8XNv1RIVr8o9fKGuvGupdT0ZdTCp_MiMBPi11OE7OCYo7iwp-yglcpOMlQF8LOCJe9txzJGqcCZSGbQBi4mNLasyYn0cstVak9ToGQpEpiSdz4FvQtSKT9A";
    // ***************** End Configuration Parameters *************
    
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var getSystem = new DisableSystem();
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
        if (args.length < 1) {
            System.out.println("Supply the system name.");
            return;
        }
        
        // Create the app.
        var sysClient = new SystemsClient(BASE_URL, userJWT);
        int count = sysClient.disableSystem(args[0]);
        System.out.println("Systems updated: " + count);
    }
}
