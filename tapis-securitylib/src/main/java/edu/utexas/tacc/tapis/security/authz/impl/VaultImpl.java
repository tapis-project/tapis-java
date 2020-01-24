package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.response.LogicalResponse;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.security.authz.model.SkSecret;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretList;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretVersion;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretVersionMetadata;
import edu.utexas.tacc.tapis.security.secrets.VaultManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This class implements all vault related calls used by the front-end.
 * 
 * @author rcardone
 */
public final class VaultImpl 
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(VaultImpl.class);

   /* ********************************************************************** */
   /*                                Fields                                  */
   /* ********************************************************************** */
   // Singleton instance of this class.
   private static VaultImpl _instance;
   
   // A list that contains a single zero integer.
   private static final ArrayList<Integer> _zeroVersionList = initZeroVersionList();
   
   /* ********************************************************************** */
   /*                             Constructors                               */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* constructor:                                                           */
   /* ---------------------------------------------------------------------- */
   private VaultImpl() {}
   
   /* ********************************************************************** */
   /*                             Public Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* getInstance:                                                           */
   /* ---------------------------------------------------------------------- */
   public static VaultImpl getInstance()
   {
       // Create the singleton instance if necessary.
       if (_instance == null) {
           synchronized (VaultImpl.class) {
               if (_instance == null) _instance = new VaultImpl();
           }
       }
       return _instance;
   }
   
   /* ---------------------------------------------------------------------- */
   /* secretRead:                                                            */
   /* ---------------------------------------------------------------------- */
   /** Read a secret on a path.  The path will be prefixed the calculated Tapis 
    * root path for the specified tenant and user.  A zero version means retrieve
    * the latest version of the secret.
    * 
    * Note that the secret key and values returned are always both strings. 
    * 
    * @param tenant caller's tenant id
    * @param user the caller
    * @param path the secret path name as seen by the user
    * @param version the specific version a the secret to retrieve
    * @return the versioned secret if it exists
    * @throws TapisImplException on error
    */
   public SkSecret secretRead(String tenant, String user, String path, 
                              Integer version)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretRead", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretRead", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(path)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretRead", "path");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (version == null) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretRead", "version");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Request Processing ------------------------
       // Calculate the secret path.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.read(secretPath, Boolean.TRUE, version);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_ERROR", 
                                        tenant, user, secretPath, version, 
                                        e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_ERROR", 
                                        tenant, user, secretPath, version, 
                                        vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // ------------------------ Request Output ----------------------------
       // Create the response object.
       var skSecret = new SkSecret();
       
       // Get the outer data which contains everything we're interested in.
       try {
           var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
           JsonObject dataObj = (JsonObject) bodyJson.get("data");
           if (dataObj != null) {
               // The inner data object is a map of zero or more key/value pairs.
               JsonObject mapObj = (JsonObject) dataObj.get("data");
               if (mapObj != null) {
                   for (var entry : mapObj.entrySet()) {
                       skSecret.secretMap.put(entry.getKey(), entry.getValue().getAsString());
                   }
               }
           
               // Get the secret metadata.
               JsonObject metaObj = (JsonObject) dataObj.get("metadata");
               if (metaObj != null) 
                   skSecret.metadata = TapisGsonUtils.getGson().fromJson(metaObj, SkSecretMetadata.class); 
           }
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_ERROR", 
                   tenant, user, secretPath, version, 
                   e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       return skSecret;
   }

   /* ---------------------------------------------------------------------- */
   /* secretWrite:                                                           */
   /* ---------------------------------------------------------------------- */
   /** Write a secret that can contain one or key/value pairs.  Even though 
    * the map allows non-string values, only string values are returned by 
    * the secretRead operation.  For consistency, it's recommended that all
    * secret values be strings.
    * 
    * The latest version of the secret written is returned.
    * 
    * @param tenant caller's tenant id
    * @param user the caller
    * @param path the secret path name as seen by the user
    * @param secretMap a map of key/value pairs
    * @return information about the newly written secret
    * @throws TapisImplException on error
    */
   public SkSecretMetadata secretWrite(String tenant, String user, String path, 
                                       Map<String, Object> secretMap)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretWrite", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretWrite", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(path)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretWrite", "path");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (secretMap == null) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretWrite", "secretMap");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           // If and when the underlying API supports the cas parameter
           // we should activate it in our code.
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.write(secretPath, secretMap);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_WRITE_SECRET_ERROR", 
                                        tenant, user,secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_WRITE_SECRET_ERROR", 
                                        tenant, user, secretPath, vaultBody);
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
       }
       
       // Return the data portion of the vault response.
       SkSecretMetadata result = null;
       try {
           var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
           result = TapisGsonUtils.getGson().fromJson(bodyJson.get("data"), SkSecretMetadata.class);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_WRITE_SECRET_ERROR", 
                   tenant, user,secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }

       return result;
   }
   
   /* ---------------------------------------------------------------------- */
   /* secretDelete:                                                          */
   /* ---------------------------------------------------------------------- */
   /** Soft delete one or more versions of a secret.  Soft deletion can be
    * reversed using undelete.  If the versions list is null or empty, the
    * request is interpreted to delete all versions.  If the list contains
    * only zero integers, it is interpreted as a request to delete the 
    * latest version only.  Otherwise, only the listed non-zero versions 
    * will be deleted.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param path the secret name
    * @param versions the versions of the secret to be deleted, can be null
    *                 or empty
    * @return the list of deleted versions
    * @throws TapisImplException
    */
   public List<Integer> secretDelete(String tenant, String user, String path,
                                     List<Integer> versions)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(path)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "path");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Version Processing ------------------------
       // Filter that allows only non-deleted, non-destroyed secret versions.
       Function <SkSecretVersion, Boolean> f = 
           t -> StringUtils.isBlank(t.deletion_time) && !t.destroyed;
       
       // The finalized versions that get processed and returned. 
       int[] versionArray = calculateVersionArray(tenant, user, path, versions, f);
       
       // Maybe there's nothing to do.
       if (versionArray.length == 0) return Collections.emptyList();
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.delete(secretPath, versionArray);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_DELETE_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_DELETE_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // Put the version array into a list.
       var outVersions = new ArrayList<Integer>(versionArray.length);
       for (int i = 0; i < versionArray.length; i++) outVersions.add(versionArray[i]);
       
       return outVersions;
   }
       
   /* ---------------------------------------------------------------------- */
   /* secretUndelete:                                                        */
   /* ---------------------------------------------------------------------- */
   /** Undelete one or more versions of a secret.  Soft deletion performed by
    * secretDelete can be reversed using this method.  If the versions list is 
    * null or empty, the request is interpreted to undelete all versions.  If 
    * the list contains only zero integers, it is interpreted as a request to 
    * undelete the latest version only.  Otherwise, only the listed non-zero 
    * versions will be undeleted.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param path the secret name
    * @param versions the versions of the secret to be undeleted, can be null
    *                 or empty
    * @return the list of undeleted versions
    * @throws TapisImplException
    */
   public List<Integer> secretUndelete(String tenant, String user, String path,
                                       List<Integer> versions)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretUndelete", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretUndelete", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(path)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretUndelete", "path");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Version Processing ------------------------
       // Filter that allows only deleted, non-destroyed secret versions.
       Function <SkSecretVersion, Boolean> f = 
           t -> StringUtils.isNotBlank(t.deletion_time) && !t.destroyed;
           
       // The finalized versions that get processed and returned. 
       int[] versionArray = calculateVersionArray(tenant, user, path, versions, f);
       
       // Maybe there's nothing to do.
       if (versionArray.length == 0) return Collections.emptyList();
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.unDelete(secretPath, versionArray);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_UNDELETE_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_UNDELETE_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // Put the version array into a list.
       var outVersions = new ArrayList<Integer>(versionArray.length);
       for (int i = 0; i < versionArray.length; i++) outVersions.add(versionArray[i]);
       
       return outVersions;
   }
       
   /* ---------------------------------------------------------------------- */
   /* secretDestroy:                                                         */
   /* ---------------------------------------------------------------------- */
   /** Hard delete one or more versions of a secret.  Hard deletion cannot be
    * reversed.  If the versions list is null or empty, the request is 
    * interpreted to destroy all versions.  If the list contains only zero 
    * integers, it is interpreted as a request to destroy the latest version only.  
    * Otherwise, only the listed non-zero versions will be destroyed.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param path the secret name
    * @param versions the versions of the secret to be destroyed, can be null
    *                 or empty
    * @return the list of destroyed versions
    * @throws TapisImplException
    */
   public List<Integer> secretDestroy(String tenant, String user, String path,
                                      List<Integer> versions)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(path)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDelete", "path");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Version Processing ------------------------
       // Filter that allows only non-destroyed secret versions.
       Function <SkSecretVersion, Boolean> f = t -> !t.destroyed;
       
       // The finalized versions that get processed and returned. 
       int[] versionArray = calculateVersionArray(tenant, user, path, versions, f);
       
       // Maybe there's nothing to do.
       if (versionArray.length == 0) return Collections.emptyList();
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.destroy(secretPath, versionArray);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_DESTROY_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_DESTROY_SECRET_VERSION_ERROR", 
                                        tenant, user, secretPath, 
                                        Arrays.toString(versionArray), vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // Put the version array into a list.
       var outVersions = new ArrayList<Integer>(versionArray.length);
       for (int i = 0; i < versionArray.length; i++) outVersions.add(versionArray[i]);
       
       return outVersions;
   }
       
   /* ---------------------------------------------------------------------- */
   /* secretReadMeta:                                                        */
   /* ---------------------------------------------------------------------- */
   /** Return the version information of a secret.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param secretName the secret name
    * @return metadata information about all versions of the secret
    * @throws TapisImplException
    */
   public SkSecretVersionMetadata secretReadMeta(String tenant, String user, String secretName)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretReadMeta", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretReadMeta", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(secretName)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretReadMeta", "secretName");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, secretName);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.list(secretPath);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // ------------------------ Request Output ----------------------------
       // Create the response object.
       var meta = new SkSecretVersionMetadata();
       
       // Populate the response object.
       try {
           // Get the outer data contains everything we're interested in.
           var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
           JsonObject dataObj = (JsonObject) bodyJson.get("data");
           if (dataObj != null) {
               // We have to go field by field because version values are returned
               // as keys, making it difficult to statically define a return type.
               meta.created_time    = dataObj.get("created_time").getAsString();
               meta.current_version = dataObj.get("current_version").getAsInt();
               meta.max_versions    = dataObj.get("max_versions").getAsInt();
               meta.oldest_version  = dataObj.get("oldest_version").getAsInt();
               meta.updated_time    = dataObj.get("updated_time").getAsString();
           
               // Get the inconveniently defined version array-like object.
               JsonObject versions  = dataObj.get("versions").getAsJsonObject();
               for (var entry : versions.entrySet()) {
                   // Initialize the version object.
                   var secretVersion = new SkSecretVersion();
               
                   // Fill in a new version number using the key. 
                   String key = entry.getKey();
                   try {secretVersion.version = Integer.valueOf(key);}
                       catch (Exception e) {
                           // This should never happen.  Log the problem and skip this entry.
                           String msg = MsgUtils.getMsg("TAPIS_RUNTIME_EXCEPTION", e.getMessage());
                           _log.error(msg, e);
                           continue;
                       }
               
                   // Fill in the rest of the fields from the entry's value.
                   JsonObject value = entry.getValue().getAsJsonObject();
                   secretVersion.created_time  = value.get("created_time").getAsString();
                   secretVersion.deletion_time = value.get("deletion_time").getAsString();
                   secretVersion.destroyed = value.get("destroyed").getAsBoolean();
               
                   // Add the version information to the list.
                   meta.versions.add(secretVersion);
               }
           }
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_METADATA_ERROR", 
                   tenant, user, secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       return meta;
   }
   
   /* ---------------------------------------------------------------------- */
   /* secretListMeta:                                                        */
   /* ---------------------------------------------------------------------- */
   /** List the secrets names defined at the specified path.  The path must
    * represent a folder.  A trailing slash will be appended if one is not 
    * present.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param path the secret folder path
    * @return metadata the list of secrets defined at the path
    * @throws TapisImplException on error
    */
   public SkSecretList secretListMeta(String tenant, String user, String path)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretListMeta", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretListMeta", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Request Processing ------------------------
       // No path is allowed as it will assigned a prefix with a trailing slash.
       // All paths ultimately need to end with a slash so that the vault driver
       // correctly triggers a vault LIST call (rather than GET).
       if (path == null) path = "";
        else if (!path.endsWith("/")) path += "/";
       
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, path);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.list(secretPath);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_LIST_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_LIST_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
       
       // ------------------------ Request Output ----------------------------
       // Create the response object.
       var secretList = new SkSecretList();
       
       // Populate the response object.
       try {
           // Get the outer data contains everything we're interested in.
           var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
           JsonObject dataObj = (JsonObject) bodyJson.get("data");
           if (dataObj != null) {
               // Marshal the list of keys.
               secretList = TapisGsonUtils.getGson().fromJson(dataObj, SkSecretList.class);
           }
           secretList.secretPath = secretPath; // do last to avoid getting stepped on
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_LIST_SECRET_METADATA_ERROR", 
                   tenant, user, secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       return secretList;
   }
   
   /* ---------------------------------------------------------------------- */
   /* secretDestroyMeta:                                                     */
   /* ---------------------------------------------------------------------- */
   /** Destroy all traces of a secret.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param secretName the secret name
    * @throws TapisImplException
    */
   public void secretDestroyMeta(String tenant, String user, String secretName)
    throws TapisImplException
   {
       // ------------------------ Input Checking ----------------------------
       if (StringUtils.isBlank(tenant)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDestroyMeta", "tenant");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(user)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDestroyMeta", "user");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       if (StringUtils.isBlank(secretName)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "secretDestroyMeta", "secretName");
           _log.error(msg);
           throw new TapisImplException(msg, Condition.BAD_REQUEST);
       }
       
       // ------------------------ Request Processing ------------------------
       // Construct the secret's full path that include tenant and user.
       String secretPath = getSecretPath(tenant, user, secretName);
       
       // Issue the vault call.
       LogicalResponse logicalResp = null;
       try {
           var logical = VaultManager.getInstance().getVault().logical();
           logicalResp = logical.delete(secretPath);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("SK_VAULT_DESTROY_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, e.getMessage());
           _log.error(msg, e);
           throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
       }
       
       // The rest response field is non-null if we get here.
       var restResp = logicalResp.getRestResponse();
       int vaultStatus = restResp.getStatus();
       String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
       
       // Did vault encounter a problem?
       if (vaultStatus >= 400) {
           String msg = MsgUtils.getMsg("SK_VAULT_DESTROY_SECRET_METADATA_ERROR", 
                                        tenant, user, secretPath, vaultBody); // this is never a secret
           _log.error(msg);
           var cond = vaultStatus >= 500 ? Condition.INTERNAL_SERVER_ERROR : Condition.BAD_REQUEST;
           throw new TapisImplException(msg, cond);       
        }
   }
   
   /* **************************************************************************** */
   /*                               Private Methods                                */
   /* **************************************************************************** */
   /* ---------------------------------------------------------------------------- */
   /* getSecretPath:                                                               */
   /* ---------------------------------------------------------------------------- */
   /** Construct the vault v2 secret pathname.
    * 
    * @param tenant the caller's tenant
    * @param user the caller userid
    * @param secretName the user-specified secret
    * @return the full secret path
    */
   private String getSecretPath(String tenant, String user, String secretName)
   {
       return "secret/tapis/" + tenant + "/" + user + "/" + secretName;
   } 
   
   /* ---------------------------------------------------------------------------- */
   /* getSecretLatestVersionNumber:                                                */
   /* ---------------------------------------------------------------------------- */
   private int getSecretLatestVersionNumber(String tenant, String user, String path) 
    throws TapisImplException
   {
       // Get the current latest version of the named secret.
       // If the secret doesn't exist or the call fails for any
       // reason, we let the exception pass through.
       //
       // Note that the latest version may be deleted or destroyed.
       SkSecretVersionMetadata meta = secretReadMeta(tenant, user, path);
       return meta.current_version;
   }
   
   /* ---------------------------------------------------------------------------- */
   /* getSecretFilteredVersionNumbers:                                             */
   /* ---------------------------------------------------------------------------- */
   private int[] getSecretFilteredVersionNumbers(String tenant, String user, String path,
                                                 Function<SkSecretVersion, Boolean> f) 
    throws TapisImplException
   {
       // Get the current latest versions of the named secret.
       // If the secret doesn't exist or the call fails for any
       // reason, we let the exception pass through.
       SkSecretVersionMetadata meta = secretReadMeta(tenant, user, path);
       
       // Find all undeleted/undestroyed versions.  Time fields can be null or empty.
       var list = new ArrayList<Integer>(meta.versions.size());
       for (var curVersion : meta.versions) {
           if (f.apply(curVersion)) list.add(curVersion.version);
       }
       
       // Put the undeleted versions (if any) in an array.
       int[] array = new int[list.size()];
       for (int i = 0; i < array.length; i++) array[i] = list.get(i);
       
       return array;
   }

   /* ---------------------------------------------------------------------------- */
   /* calculateVersionArray:                                                       */
   /* ---------------------------------------------------------------------------- */
   /** Determine which secret versions will be processed based on user input.
    * 
    * @param tenant the callers tenant
    * @param user the caller
    * @param path the secret name
    * @param versions the versions of the secret to be undeleted, can be null
    *                 or empty
    * @return the scrubbed array of version numbers
 * @throws TapisImplException 
    */
   private int[] calculateVersionArray(String tenant, String user, String path,
                                       List<Integer> versions,
                                       Function<SkSecretVersion, Boolean> f) 
     throws TapisImplException
   {
       // The finalized versions that get processed and returned. 
       int[] versionArray;
       
       // Empty or null version input means delete all versions.
       if (versions == null || versions.isEmpty()) {
           versionArray = getSecretFilteredVersionNumbers(tenant, user, path, f);
       } else {
           // Scrub the input list of all zero elements.
           var vlist = new ArrayList<Integer>(versions);
           vlist.removeAll(_zeroVersionList);

           // Determine if we are deleting the latest or specific versions.
           if (vlist.isEmpty()) {
               // The input list contained only zeros before scrubbing.
               versionArray = new int[1];
               versionArray[0] = getSecretLatestVersionNumber(tenant, user, path);
           } else {
               // Use what the user specified ignoring zero if it's present.
               // If we get here, there's at least one non-zero version specified.
               versionArray = new int[vlist.size()];
               for (int i = 0; i < versionArray.length; i++) versionArray[i] = vlist.get(i);
           }
       }
       
       return versionArray;
   }
   
   /* ---------------------------------------------------------------------------- */
   /* initZeroVersionList:                                                         */
   /* ---------------------------------------------------------------------------- */
   private static ArrayList<Integer> initZeroVersionList()
   {
       // Create the list containing a single zero integer.
       var zeroList = new ArrayList<Integer>(1);
       zeroList.add(0);
       return zeroList;
       
   }
}