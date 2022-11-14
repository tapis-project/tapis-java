package edu.utexas.tacc.tapis.security.commands.aux.export;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.TreeSet;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.security.secrets.SecretTypeDetector;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** Export secrets from Vault in either raw or deployment ready formats.
 * 
 * @author rcardone
 */
public class SkExport 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Root of the tapis secrets subtree.
    private static final String TAPIS_SECRET_ROOT = "tapis/";
    
    // Initial output string.
    private static final String START_SECRETS = "[";
    private static final int    START_SECRETS_LEN = START_SECRETS.length();
    private static final String END_SECRETS  = "\n]";
    private static final int    OUTPUT_BUFLEN = 8192;

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // User input.
    private final SkExportParameters   _parms;
    
    // The client used for all http calls.
    private final HttpClient           _httpClient;
    
    // Raw secrets information.
    private final ArrayList<SecretInfo> _secretRecs;
    
    // Progress counters.
    private int                        _numListings;
    private int                        _numReads;
    private int                        _numUnknownPaths;
    
    // Result reporting lists.
    private final TreeSet<String>      _failedReads;   // Number of secrets paths that could not be read.
    
    /* ********************************************************************** */
    /*                                 Records                                */
    /* ********************************************************************** */
    // Wrapper for secret info.
    private record SecretInfo(SecretType type, String path, String secret) {}
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkExport(SkExportParameters parms) 
    {
        // Parameters cannot be null.
        if (parms == null) {
          String msg = "SkExport requires a parameter object.";
          throw new IllegalArgumentException(msg);
        }
        
        // Initialize final fields.
        _parms = parms;
        _httpClient  = HttpClient.newHttpClient();
        _failedReads = new TreeSet<String>();
        _secretRecs  = new ArrayList<>(256);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    /** No logging necessary in this method since the called methods log errors.
     * 
     * @param args the command line parameters
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception 
    {
        // Parse the command line parameters.
        SkExportParameters parms = null;
        parms = new SkExportParameters(args);
        
        // Start the worker.
        SkExport skAdmin = new SkExport(parms);
        skAdmin.export();
    }

    /* ---------------------------------------------------------------------- */
    /* export:                                                                */
    /* ---------------------------------------------------------------------- */
    public void export()
     throws Exception
    {
        // Check status of Vault.
        checkVaultStatus();
        
        // Walk the Vault source tree and discover all tapis secrets.
        processSourceTree(TAPIS_SECRET_ROOT);
        
        // Output.
        writeResults();
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* out:                                                                   */
    /* ---------------------------------------------------------------------- */
    private void out(String s) {if (!_parms.quiet) System.out.println(s);}
    
    /* ---------------------------------------------------------------------- */
    /* processSourceTree:                                                     */
    /* ---------------------------------------------------------------------- */
    /** The first call to this recursive method starts at the root of the tapis
     * hierarchy in Vault. 
     * 
     * @param curpath the path to explore depth-first
     */
    private void processSourceTree(String curpath) throws Exception
    {
        // Increment listing counter.
        _numListings++;
        
        // Make the request to list the curpath.
        HttpRequest request;
        HttpResponse<String> resp;
        try {
            request = HttpRequest.newBuilder()
                .uri(new URI(_parms.vurl + "v1/secret/metadata/" + curpath))
                .headers("X-Vault-Token", _parms.vtok, "Accept", "application/json", 
                         "Content-Type", "application/json")
                .method("LIST", BodyPublishers.noBody())
                .build();
            resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Record read failure and display error message.
            recordFailedRead(_parms.vurl + "v1/secret/metadata/" + curpath);
            out(e.getClass().getSimpleName() + ": " + e.getMessage());
            return;
        }
        
        // Check return code.
        int rc = resp.statusCode();
        if (rc == 404) {
            // We probably discovered a secret (i.e., leaf node).
            copySecret(curpath);
            return;
        }
        else if (rc >= 300) {
            // Looks like an error.
            recordFailedRead(_parms.vurl + "v1/secret/metadata/" + curpath);
            out("Received http status code " + rc + " on LIST request to " + 
                "source vault: " + request.uri().toString() + ".");
            return;
        } else {
            // Intermediate node. Parse the response body that looks something like this:
            // {"data": {"keys": ["foo", "foo/"]}}
            var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
            var data    = jsonObj.get("data").getAsJsonObject();
            var keys    = data.get("keys").getAsJsonArray();
            int numKeys = keys.size();
            for (int i = 0; i < numKeys; i++) {         
               String key = keys.get(i).getAsString();
               processSourceTree(curpath + key);
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* copySecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private void copySecret(String curpath)
    {
        // Get the secret from the source vault.
        var secretText = readSecret(curpath);
        if (secretText == null) return;
        
        // Do we care about this path?
        // We may not need to export all secrets.
        var typeWrapper = new SecretTypeWrapper(); 
        if (skipStore(curpath, typeWrapper)) return;
        
        // Collect the path and secret.
        var r = new SecretInfo(typeWrapper._secretType, curpath, secretText);
        _secretRecs.add(r);
        
        // Accumulate the secrets written.
        if (_numReads % 500 == 0) 
            out("->Listings = " + _numListings 
                + ",\tReads = "  + _numReads);
    }
    
    /* ---------------------------------------------------------------------- */
    /* readSecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private String readSecret(String secretPath)
    {
        // Increment listing counter.
        _numReads++;
        
        // Make the request.
        HttpRequest request;
        HttpResponse<String> resp;
        try {
            request = HttpRequest.newBuilder()
                .uri(new URI(_parms.vurl + "v1/secret/data/" + secretPath))
                .headers("X-Vault-Token", _parms.vtok, "Accept", "application/json", 
                         "Content-Type", "application/json")
                .build();
            resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Record read failure and display error message.
            recordFailedRead(_parms.vurl + "v1/secret/data/" + secretPath);
            out(e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
        
        // Check return code.
        int rc = resp.statusCode();
        if (rc >= 300) {
            // Looks like an error.
            recordFailedRead(_parms.vurl + "v1/secret/data/" + secretPath);
            out("Received http status code " + rc + " on READ request to " + 
                "source vault: " + request.uri().toString() + ".");
            return null;
        } 
        
        // Parse the response body and return the value of the data object.
        // The secrets look like:  "data": {"data": {"foo": "bar"}, "metadata": {..}}
        JsonObject jsonObj =  TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
        var dataObj = jsonObj.get("data").getAsJsonObject();
        if (dataObj == null) return null;
          else return dataObj.get("data").toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* skipStore:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Based on the output settings determine whether we ignore this record. 
     * 
     * @param secretPath the path of the secret.
     * @param resultType output variable that captures the secret type.
     * @return true if secret should be skipped, false to store and process.
     */
    private boolean skipStore(String secretPath, SecretTypeWrapper resultType)
    {
        // Is this a full dump of all secrets?
        if (!_parms.skipUserSecrets) return false;
        
        // Determine if this is a user secret.
        var secretType = SecretTypeDetector.detectType(secretPath);
        if (secretType == null) {
            _numUnknownPaths++;
            return true; // skip
        }
        if (secretType == SecretType.System || secretType == SecretType.User) 
            return true; // skip
        
        // Don't skip writing this secret.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordFailedRead:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Add a source read failure record.
     * 
     * @param path the complete path on which the read was attempted
     */
    private void recordFailedRead(String path)
    {
        try {
            URI uri = new URI(path);
            _failedReads.add(uri.toString());
        } catch (Exception e1) {_failedReads.add(path);}
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkVaultStatus:                                                      */
    /* ---------------------------------------------------------------------- */
    private void checkVaultStatus() throws Exception
    {
        // Get vault information.
        String baseUrl = _parms.vurl;
        String tok = _parms.vtok;
        
        // Issue request.
        HttpRequest request = HttpRequest.newBuilder()
            .uri(new URI(baseUrl + "v1/sys/health"))
            .headers("X-Vault-Token", tok, "Accept", "application/json", 
                     "Content-Type", "application/json")
            .GET()
            .build();
        HttpResponse<String> resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Check status code.
        int rc = resp.statusCode();
        if (rc >= 300) {
            String msg = "Received http status code " + rc + " on request to " +
                         "vault: " + request.uri().toString() + ".";
            throw new RuntimeException(msg);
        }
        
        // Parse the response body.
        var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
        if (jsonObj == null) {
            String msg = "Received http status code " + rc + " and no response content " +
                         "on request to vault: " + request.uri().toString() + ".";
            throw new RuntimeException(msg);
        }
        boolean sealed = jsonObj.get("sealed").getAsBoolean();
        String version = jsonObj.get("version").getAsString();
        out("Vault at " + baseUrl + "is at version " + version + 
            " and is " + (sealed ? "" : "not ") + "sealed.");
        if (sealed) {
            String msg = "Unable to continue because vault at " + baseUrl + " is sealed.";
            throw new RuntimeException(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* writeResults:                                                          */
    /* ---------------------------------------------------------------------- */
    private void writeResults()
    {
        // Populate a json object will all the secrets
        // in the user-specified format.
        String secrets = writeSecrets();
        
        // Did we encounter unknown paths?
        var unknownPathMsg = _numUnknownPaths == 0 ? "" : " <-- INVESTIGATE";
        
        // Print summary information.
        var numWrites = _secretRecs.size();
        out("\n-------------------------------------------------");
        out("Attempted listings = " + _numListings + ", attempted reads = " + _numReads);
        out("Unknown paths encountered = " + _numUnknownPaths + unknownPathMsg);
        out("Secrets written = " + numWrites + ", secrets skipped = " + (_numReads - numWrites));
        if (!_failedReads.isEmpty()) {
            out("\n-------------------------------------------------");
            out("Failed secret reads: " + _failedReads.size() + "\n");
            var it = _failedReads.iterator();
            while (it.hasNext()) out("  " + it.next());
        }
        
        // Print secrets.
        out("\n-------------------------------------------------");
        out("****** SECRETS ******");
        System.out.println(secrets); // Always write the secrets.
    }

    /* ---------------------------------------------------------------------- */
    /* writeSecrets:                                                          */
    /* ---------------------------------------------------------------------- */
    private String writeSecrets()
    {
        // Initialize result json string.
        var secrets = new StringBuilder(OUTPUT_BUFLEN);
        secrets.append(START_SECRETS);
        
        // Write each path/secret pair as json. The secret is itself a json 
        // object so the result is that secret is nested in the result object.
        // When raw output is requested, the result ends up looking like this:
        //
        // {
        //    "key": "tapis/service/postgres/dbhost/sk-postgres/dbname/tapissecdb/dbuser/tapis/credentials/passwords",
        //    "value": { "password": "abcdefg" }
        // }
        //
        // When raw output is not requested, the key is converted into a string derived 
        // from the raw path and appropriate for use as an environment variable name. 
        for (var rec: _secretRecs) {
            // Use the raw path or convert it to an env variable name.
            String key = _parms.rawDump ? rec.path() : makeKey(rec.path());
            
            // Format the json payload.
            if (secrets.length() != START_SECRETS_LEN) secrets.append(",");
            secrets.append("\n{\"key\": \"");
            secrets.append(key);
            secrets.append("\",\"value\":");
            secrets.append(rec.secret());
            secrets.append("}");
        }
        
        // Close the secrets outer json object and return.
        secrets.append(END_SECRETS);
        return secrets.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeKey:                                                               */
    /* ---------------------------------------------------------------------- */
    private String makeKey(String rawPath)
    {
        return null;
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    // Temporary holder for a secret type.
    private static final class SecretTypeWrapper {
        private SecretType _secretType;
    }
}
