package edu.utexas.tacc.tapis.security.commands.aux.transfer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.TreeSet;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This program can be used to transfer Tapis secrets from one Vault instance to
 * another.  The process is to walk the source Vault KV v2 tree starting at 
 * secret/tapis and write all secrets it discovers to the target Vault instance.  
 * 
 * The source Vault's token can be any token that has access to the KV tree.  For
 * example, the root token could be used.  On the other hand, the target Vault's 
 * token should be the same token that SK would use during normal execution.  The
 * target token should be an AppRole generated token using the "sk" role-id.  
 * 
 * @author rcardone
 */
public class TransferSecrets 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Root of the tapis secrets subtree.
    private static final String TAPIS_SECRET_ROOT = "tapis/";

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private enum VaultInstance {source, target}
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // User input.
    private final TransferSecretsParms _parms;
    
    // The client used for all http calls.
    private final HttpClient           _httpClient;
    
    // Progress counters.
    private int                        _numListings;
    private int                        _numReads;
    
    // Result reporting lists.
    private final TreeSet<String>      _successWrites; // Number of secrets written to target.
    private final TreeSet<String>      _failedWrites;  // Number of secrets that failed write to target.
    private final TreeSet<String>      _failedReads;   // Number of secrets paths that could not be read.
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public TransferSecrets(TransferSecretsParms parms) 
    {
        // Parameters cannot be null.
        if (parms == null) {
          String msg = "TransferSecrets requires a parameter object.";
          throw new IllegalArgumentException(msg);
        }
        
        // Initialize final fields.
        _parms = parms;
        _httpClient = HttpClient.newHttpClient();
        _successWrites = new TreeSet<String>();
        _failedWrites  = new TreeSet<String>();
        _failedReads   = new TreeSet<String>();
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
        TransferSecretsParms parms = null;
        parms = new TransferSecretsParms(args);
        
        // Start the worker.
        TransferSecrets trans = new TransferSecrets(parms);
        trans.transfer();
    }

    /* ---------------------------------------------------------------------- */
    /* transfer:                                                              */
    /* ---------------------------------------------------------------------- */
    public void transfer() throws Exception
    {
        // Check status of source and target Vaults.
        checkStatus(VaultInstance.source);
        checkStatus(VaultInstance.target);
        
        // Walk the source tree and discover all tapis secrets.
        processSourceTree(TAPIS_SECRET_ROOT);
        
        // Output.
        writeResults();
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* processSourceTree:                                                     */
    /* ---------------------------------------------------------------------- */
    /** The first call to this method
     * 
     * @param curpath
     * @return
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
                .uri(new URI(_parms.surl + "v1/secret/metadata/" + curpath))
                .headers("X-Vault-Token", _parms.stok, "Accept", "application/json", 
                         "Content-Type", "application/json")
                .method("LIST", BodyPublishers.noBody())
                .build();
            resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Record read failure and display error message.
            recordFailedRead(_parms.surl + "v1/secret/metadata/" + curpath);
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
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
            recordFailedRead(_parms.surl + "v1/secret/metadata/" + curpath);
            System.out.println("Received http status code " + rc + " on LIST request to " + 
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
        
        return;
    }
    
    /* ---------------------------------------------------------------------- */
    /* copySecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private void copySecret(String curpath)
    {
        // Get the secret from the source vault.
        var secretText = readSecret(curpath);
        if (secretText == null) return;
        
        // Write the secret to the target vault.
        writeSecret(curpath, secretText);
        
        // Accumulate the secrets written.
        if (_numReads % 500 == 0) 
            System.out.println("->Listings = " + _numListings 
                               + ",\tReads = "  + _numReads 
                               + ",\tWrites = " + _successWrites.size());
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
                .uri(new URI(_parms.surl + "v1/secret/data/" + secretPath))
                .headers("X-Vault-Token", _parms.stok, "Accept", "application/json", 
                         "Content-Type", "application/json")
                .build();
            resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Record read failure and display error message.
            recordFailedRead(_parms.surl + "v1/secret/data/" + secretPath);
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
        
        // Check return code.
        int rc = resp.statusCode();
        if (rc >= 300) {
            // Looks like an error.
            recordFailedRead(_parms.surl + "v1/secret/data/" + secretPath);
            System.out.println("Received http status code " + rc + " on READ request to " + 
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
    /* writeSecret:                                                           */
    /* ---------------------------------------------------------------------- */
    private void writeSecret(String secretPath, String secretText)
    {
        // Format the json payload.
        secretText = "{\"data\":" + secretText + "}";
        
        // Make the request.
        HttpRequest request;
        HttpResponse<String> resp;
        try {
            request = HttpRequest.newBuilder()
                .uri(new URI(_parms.turl + "v1/secret/data/" + secretPath))
                .headers("X-Vault-Token", _parms.ttok, "Accept", "application/json", 
                         "Content-Type", "application/json")
                .POST(BodyPublishers.ofString(secretText))
                .build();
            
            // Assume that the write would have worked.
            if (_parms.dryRun) {
                _successWrites.add(request.uri().toString());
                return;
            }
            
            // Write to target.
            resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Record failure and output error message.
            recordFailedWrite(_parms.turl + "v1/secret/data/" + secretPath);
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
            return;
        }
        
        // Check return code.
        int rc = resp.statusCode();
        if (rc >= 300) {
            // Looks like an error.
            recordFailedWrite(_parms.turl + "v1/secret/data/" + secretPath);
            System.out.println("Received http status code " + rc + " on WRITE request to " + 
                               "target vault: " + request.uri().toString() + ".");
        } 
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
    /* recordFailedWrite:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Add a target write failure record.
     * 
     * @param path the complete path on which the write was attempted
     */
    private void recordFailedWrite(String path)
    {
        try {
            URI uri = new URI(path);
            _failedWrites.add(uri.toString());
        } catch (Exception e1) {_failedWrites.add(path);}
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkStatus:                                                           */
    /* ---------------------------------------------------------------------- */
    private void checkStatus(VaultInstance vaultInstance) throws Exception
    {
        // Determine target.
        String baseUrl, tok;
        if (vaultInstance == VaultInstance.source) {
            baseUrl = _parms.surl;
            tok = _parms.stok;
        }
        else {
            baseUrl = _parms.turl;
            tok = _parms.ttok;
        }
        
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
            String msg = "Received http status code " + rc + " on request to " + vaultInstance.name() +
                         " vault: " + request.uri().toString() + ".";
            throw new RuntimeException(msg);
        }
        
        // Parse the response body.
        var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
        if (jsonObj == null) {
            String msg = "Received http status code " + rc + " and no response content " +
                         "on request to " + vaultInstance.name() +
                         " vault: " + request.uri().toString() + ".";
            throw new RuntimeException(msg);
        }
        boolean sealed = jsonObj.get("sealed").getAsBoolean();
        String version = jsonObj.get("version").getAsString();
        System.out.println("Vault at " + baseUrl + " (" + vaultInstance.name() + ") " +
                           "is at version " + version + " and is " + (sealed ? "" : "not ") +
                           "sealed.");
        if (sealed) {
            String msg = "Unable to continue because " + vaultInstance.name() + " vault at " +
                         baseUrl + " is sealed.";
            throw new RuntimeException(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* writeResults:                                                          */
    /* ---------------------------------------------------------------------- */
    private void writeResults()
    {
        System.out.println("\n-------------------------------------------------");
        System.out.println("Attempted listings = " + _numListings + ", attempted reads = " + _numReads);
        if (_parms.dryRun) System.out.println("DRY RUN - secrets that would be copied: " + _successWrites.size() + "\n");
          else System.out.println("Total secrets copied to target Vault: " + _successWrites.size() + "\n");
        if (!_successWrites.isEmpty()) {
            var it = _successWrites.iterator();
            while (it.hasNext()) System.out.println("  " + it.next());
        }
        
        if (!_failedWrites.isEmpty()) {
            System.out.println("\n-------------------------------------------------");
            System.out.println("Failed secret writes: " + _failedWrites.size() + "\n");
            var it = _failedWrites.iterator();
            while (it.hasNext()) System.out.println("  " + it.next());
        }
        
        if (!_failedReads.isEmpty()) {
            System.out.println("\n-------------------------------------------------");
            System.out.println("Failed secret reads: " + _failedReads.size() + "\n");
            var it = _failedReads.iterator();
            while (it.hasNext()) System.out.println("  " + it.next());
        }
    }
}
