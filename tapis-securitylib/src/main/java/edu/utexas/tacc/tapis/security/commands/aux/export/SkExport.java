package edu.utexas.tacc.tapis.security.commands.aux.export;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.security.commands.aux.export.SkExportParameters.OutputFormat;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper;
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
    
    // We split vault paths on slashes.
    private static final Pattern SPLIT_PATTERN = Pattern.compile("/");
    
    // We sanitize by removing all characters not in this character class.
    private static final Pattern SANITIZER = Pattern.compile("[^a-zA-Z0-9_]");

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
    // Wrapper for raw secret info.
    private record SecretInfo(SecretType type, String path, String secret) {}
    
    // Wrapper for processed SecretInfo records.
    private record SecretOutput(String key, String value) {}
    
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
        
        // Put all secrets into a list of records.
        var outputRecs = calculateOutputRecs();
        
        // Put the raw data into the user-specified output format.
        writeResults(outputRecs);
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* out:                                                                   */
    /* ---------------------------------------------------------------------- */
    private void out(String s) {if (_parms.verbose) System.out.println(s);}
    
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
        // Always parse the path.
        var secretType = SecretTypeDetector.detectType(secretPath);
        if (secretType == null) {
            _numUnknownPaths++;
            return true; // skip
        }
        
        // Pass back the result secret type.
        resultType._secretType = secretType;
        
        // Is this a full dump of all secrets?
        if (_parms.noSkipUserSecrets) return false;
        
        // Determine if this is a user-initiated secret.
        if (secretType == SecretType.System || secretType == SecretType.User) 
            return true; // skip
        
        // Don't skip writing this secret.
        return false;
    }

    /* ---------------------------------------------------------------------- */
    /* calculateOutputRecs:                                                   */
    /* ---------------------------------------------------------------------- */
    private List<SecretOutput> calculateOutputRecs()
    {
        // Estimate the output list size based on the number of raw secrets.
        var olist = new ArrayList<SecretOutput>(2*_secretRecs.size());
        
        // Each raw record can create one or more output records.
        for (var srec : _secretRecs) {
            // The easy case is when we return Vault's output as is.
            if (_parms.format != OutputFormat.ENV) {
                olist.add(getRawDumpOutputRec(srec));
                continue;
            }
            
            // Parse json record and add record(s) to output list in preparation
            // for ENV output.  By default the key are sanitized.
            switch (srec.type) {
                case ServicePwd:   getServicePwdOutputRec(srec, olist); break;
                case DBCredential: getDBCredentialOutputRec(srec, olist); break;
                case JWTSigning:   getJWTSigningOutputRec(srec, olist); break;
                case System:       getSystemOutputRec(srec, olist); break;
                case User:         getUserOutputRec(srec, olist); break;
                default:
            }
        }
        
        // Return the list.
        return olist;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getServicePwdOutputRec:                                                */
    /* ---------------------------------------------------------------------- */
    private void getServicePwdOutputRec(SecretInfo srec, List<SecretOutput> olist)
    {
        // Construct the key string based on the user-selected output format.
        // Split the path into segments.  We know the split is valid since it 
        // already passed muster in SecretTypeDetector. The service name is 
        // at index 4.
        var parts = SPLIT_PATTERN.split(srec.path(), 0);
        String keyPrefix = SecretType.ServicePwd.name().toUpperCase() + "_" +
                           parts[4].toUpperCase(); 
        addDynamicSecrets(keyPrefix, srec.secret(), olist);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDBCredentialOutputRec:                                              */
    /* ---------------------------------------------------------------------- */
    private void getDBCredentialOutputRec(SecretInfo srec, List<SecretOutput> olist)
    {
        // Construct the key string based on the user-selected output format.
        // Split the path into segments.  We know the split is valid since it 
        // already passed muster in SecretTypeDetector. The service name is 
        // at index 2, dbhost at 4, dbname at 6, dbuser at 8. 
        var parts = SPLIT_PATTERN.split(srec.path(), 0);
        String keyPrefix = SecretType.DBCredential.name().toUpperCase() + "_" +
                           parts[2].toUpperCase() + "_" + 
                           parts[4].toUpperCase() + "_" +
                           parts[6].toUpperCase() + "_" +
                           parts[8].toUpperCase(); 
        addDynamicSecrets(keyPrefix, srec.secret(), olist);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJWTSigningOutputRec:                                                */
    /* ---------------------------------------------------------------------- */
    private void getJWTSigningOutputRec(SecretInfo srec, List<SecretOutput> olist)
    {
        // Construct the key string based on the user-selected output format.
        // Split the path into segments.  We know the split is valid since it 
        // already passed muster in SecretTypeDetector. The tenant name is 
        // at index 2. 
        var parts = SPLIT_PATTERN.split(srec.path(), 0);
        
        // Process both public and private keys.
        String keyPrefix = SecretType.JWTSigning.name().toUpperCase() + "_" +
                           parts[2].toUpperCase(); 
        addKeyPair(keyPrefix, srec.secret(), olist);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSystemOutputRec:                                                    */
    /* ---------------------------------------------------------------------- */
    private void getSystemOutputRec(SecretInfo srec, List<SecretOutput> olist)
    {
        // Construct the key string based on the user-selected output format.
        // Split the path into segments.  We know the split is valid since it 
        // already passed muster in SecretTypeDetector. The tenant name is 
        // at index 2, the system id at 4.  
        var parts = SPLIT_PATTERN.split(srec.path(), 0);
        
        // Set the key prefix.
        String keyPrefix = SecretType.System.name().toUpperCase() + "_" +
                           parts[2].toUpperCase() + "_" + 
                           parts[4].toUpperCase() + "_";
        
        // We need to know if we are using a dynamic or static user to complete the key.
        String keyType, key;
        if ("dynamicUserId".equals(parts[5])) {
            // Capture the authn type and the static dynamic user string.
            keyType = parts[6];
            key = keyPrefix + "DYNAMICUSERID";
        } else {
            // Capture the authn type and user.
            keyType = parts[7];
            key = keyPrefix + parts[6].toUpperCase();
        }
        
        // Lock down the key type.
        SecretPathMapper.KeyType keyTypeEnum = null;
        try {keyTypeEnum = SecretPathMapper.KeyType.valueOf(keyType);}
            catch (Exception e) {
                out(srec.path() + " has invalid keyType: " + keyType + ".\n" + e.toString());
                return;
            }
        
        // Assign the value based on the key type.
        switch (keyTypeEnum) {
            case sshkey:
            case cert:
                addKeyPair(key, srec.secret(), olist);
            break;
            
            case password:
            case accesskey:
                addDynamicSecrets(key, srec.secret(), olist);
            break;
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getUserOutputRec:                                                      */
    /* ---------------------------------------------------------------------- */
    private void getUserOutputRec(SecretInfo srec, List<SecretOutput> olist)
    {
        // Construct the key string based on the user-selected output format.
        // Split the path into segments.  We know the split is valid since it 
        // already passed muster in SecretTypeDetector. The tenant name is 
        // at index 2, user at 4, secretName at 6.
        var parts = SPLIT_PATTERN.split(srec.path(), 0);
        String keyPrefix = SecretType.User.name().toUpperCase() + "_" +
                           parts[2].toUpperCase() + "_" +
                           parts[4].toUpperCase() + "_" +
                           parts[6].toUpperCase();
        addDynamicSecrets(keyPrefix, srec.secret(), olist);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addDynamicSecrets:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create an output entry for each key/value pair listed in the Vault
     * secret text.
     * 
     * @param keyPrefix the prefix of the attribute we'll create
     * @param rawSecret the Vault secret value as json text
     * @param olist the result accumulator
     */
    private void addDynamicSecrets(String keyPrefix, String rawSecret, List<SecretOutput> olist)
    {
        // Replace env unfriendly character in the prefix.
        if (!_parms.noSanitizeName) keyPrefix = sanitize(keyPrefix);
        
        // Dynamically discover the individual values associated with this 
        // user secret.  Since the keys are user chosen, we may have to transform 
        // them to avoid illegal characters in target context (e.g., env variables).
        // First let's see if there's any secret.
        if (rawSecret == null) {
            olist.add(new SecretOutput(keyPrefix, ""));
            return;
        }
        
        // The keys are at the top level in the json object.
        // We process the private key first.
        JsonObject jsonObj = TapisGsonUtils.getGson().fromJson(rawSecret, JsonObject.class);
        for (var entry : jsonObj.entrySet()) {
            var key = entry.getKey();
            if (!_parms.noSanitizeName) key = sanitize(key); 
            var val = entry.getValue().getAsString();
            if (val == null) val = "";
            olist.add(new SecretOutput(keyPrefix + "_" + key.toUpperCase(), val));
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addKeyPair:                                                            */
    /* ---------------------------------------------------------------------- */
    /** This specialized version of addDynamicSecrets ignores the "key" attribute
     * that Vault return in add to privateKey and publicKey.  Basically, we
     * hardcode the two attributes we're interested in and remove extraneous 
     * quotes from the value string.
     * 
     * @param keyPrefix the prefix of the attribute we'll create
     * @param rawSecret the Vault secret value as json text
     * @param olist the result accumulator
     */
    private void addKeyPair(String keyPrefix, String rawSecret, List<SecretOutput> olist)
    {
        // Replace env unfriendly character in the prefix.
        if (!_parms.noSanitizeName) keyPrefix = sanitize(keyPrefix);
        
        // The keys are at the top level in the json object.
        // We process the private key first.
        String value = null;
        JsonObject jsonObj = null;
        if (rawSecret != null) {
            jsonObj = TapisGsonUtils.getGson().fromJson(rawSecret, JsonObject.class);
            value = jsonObj.get("privateKey").toString();
        }
        
        // Massage the value.
        if (value == null) value = "";
         else {
             // For some reason there are double quotes around the secret string.
             if (value.startsWith("\"")) value = value.substring(1);
             if (value.endsWith("\"")) value = value.substring(0, value.length()-1);
         }
        
        // Construct the record.
        olist.add(new SecretOutput(keyPrefix + "_PRIVATEKEY", value));
        
        // Next process the public key.
        if (jsonObj != null) value = jsonObj.get("publicKey").toString();
        if (value == null) value = "";
        else {
            // For some reason there are double quotes around the secret string.
            if (value.startsWith("\"")) value = value.substring(1);
            if (value.endsWith("\"")) value = value.substring(0, value.length()-1);
        }
        
        // Construct the record.
        olist.add(new SecretOutput(keyPrefix + "_PUBLICKEY", value));
    }
    
    /* ---------------------------------------------------------------------- */
    /* sanitize:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Replace all characters not in the sanitizer character class with underscore.
     * 
     * @param s string to be sanitized
     * @return sanitized string
     */
    private String sanitize(String s)
    {
        if (s == null) return s;
        return SANITIZER.matcher(s).replaceAll("_");
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRawDumpOutputRec:                                                   */
    /* ---------------------------------------------------------------------- */
    private SecretOutput getRawDumpOutputRec(SecretInfo srec)
    {
        return new SecretOutput(srec.path(), srec.secret());
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
    private void writeResults(List<SecretOutput> olist)
    {
        // Populate a json object will all the secrets
        // in the user-specified format.
        String secrets = _parms.format == OutputFormat.JSON ? 
                               writeJsonOutput(olist) : writeEnvOutput(olist);
        
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
    /* writeJsonOutput:                                                       */
    /* ---------------------------------------------------------------------- */
    private String writeJsonOutput(List<SecretOutput> olist)
    {
        // Initialize result json string.
        var secrets = new StringBuilder(OUTPUT_BUFLEN);
        secrets.append(START_SECRETS);
        
        // Write each path/secret pair as json. The secret is itself a json object 
        // so the result is that secret is nested in the result object. When raw 
        // output is requested, the result objects end up looking like this:
        //
        // {
        //    "key": "tapis/service/postgres/dbhost/sk-postgres/dbname/tapissecdb/dbuser/tapis/credentials/passwords",
        //    "value": { "password": "abcdefg" }
        // }
        //
        // When raw output is not requested, the key is converted into a string derived 
        // from the raw path and appropriate for use as an environment variable name. 
        for (var rec: olist) {
            // Format the json payload.
            if (secrets.length() != START_SECRETS_LEN) secrets.append(",");
            secrets.append("\n{\"key\": \"");
            secrets.append(rec.key());
            secrets.append("\",\"value\":");
            secrets.append(rec.value());
            secrets.append("}");
        }
        
        // Close the secrets outer json object and return.
        secrets.append(END_SECRETS);
        return secrets.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* writeEnvOutput:                                                        */
    /* ---------------------------------------------------------------------- */
    private String writeEnvOutput(List<SecretOutput> olist)
    {
        // Initialize result json string.
        var secrets = new StringBuilder(OUTPUT_BUFLEN);
        
        // Write each path/secret pair in environment variable format. The secret 
        // key is a name derived from the secret's Vault path and the value is 
        // the secret itself. The result lines end up looking like this:
        //
        //    SOME_ENV_NAME='abcdefg'
        //
        for (var rec: olist) {
            // Format the json payload.
            secrets.append(rec.key());
            secrets.append("=");
            if (_parms.quoteEnvValues) secrets.append("'");
            secrets.append(rec.value());
            if (_parms.quoteEnvValues) secrets.append("'");
            secrets.append("\n");
        }
        
        // Close the secrets outer json object and return.
        return secrets.toString();
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    // Temporary holder for a secret type.
    private static final class SecretTypeWrapper {
        private SecretType _secretType;
    }
}
