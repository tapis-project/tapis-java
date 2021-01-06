package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.ArgMetaSpec;
import edu.utexas.tacc.tapis.shared.model.ArgSpec;
import edu.utexas.tacc.tapis.shared.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.shared.model.JobParameterSet;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;

public final class JobParmSetMarshaller 
{
    /* **************************************************************************** */
    /*                                 Constants                                    */
    /* **************************************************************************** */
    // Environment variable names that start with this prefix are reserved for Tapis.
    private static final String TAPIS_ENV_VAR_PREFIX = "_tapis";
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* marshalAppParmSet:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of ParameterSet with the generated
     * data passed by the apps and systems services.  These inputs have a different
     * package type but are otherwise the same as the corresponding types in the 
     * shared library.  
     * 
     * Note that we trust the app and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param appParmSet the parameterSet retrieved from the app definition.
     * @param sysEnv the environment variable list from systems
     * @return the populate sharedlib parameterSet object, never null
     * @throws TapisImplException 
     */
    public JobParameterSet marshalAppParmSet(
        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSet appParmSet,
        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysEnv) 
     throws TapisImplException
    {
        // Always create a new parameter set.
        var parmSet = new JobParameterSet();
        if (appParmSet == null) return parmSet;
        
        // Null can be returned from the marshal method.
        var appAppArgs = appParmSet.getAppArgs();
        parmSet.setAppArgs(marshalAppArgSpecList(appAppArgs));
        
        // Null can be returned from the marshal method.
        var appContainerArgs = appParmSet.getContainerArgs();
        parmSet.setContainerArgs(marshalAppArgSpecList(appContainerArgs));
        
        // Null can be returned from the marshal method.
        var appSchedulerOptions = appParmSet.getSchedulerOptions();
        parmSet.setSchedulerOptions(marshalAppArgSpecList(appSchedulerOptions));
        
        // Null can be returned from the marshal method.
        var appEnvVariables = appParmSet.getEnvVariables();
        parmSet.setEnvVariables(marshalAppKvList(appEnvVariables, sysEnv));
        
        // Null can be returned from the marshal method.
        var appArchiveFilter = appParmSet.getArchiveFilter();
        parmSet.setArchiveFilter(marshalAppAchiveFilter(appArchiveFilter));
        
        return parmSet;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeParmSets:                                                               */
    /* ---------------------------------------------------------------------------- */
    /** Merge each of the individual components of the application/system parameter set
     * into the request parameter set. 
     * 
     * @param reqParmSet non-null parameters from job request, used for input and output
     * @param appParmSet non-null parameters from application and system, input only
     * @throws TapisImplException
     */
    public void mergeParmSets(JobParameterSet reqParmSet, JobParameterSet appParmSet) 
     throws TapisImplException
    {
        // Initialize all fields in the request parameter set. This guarantees
        // that all fields are non-null at all depth levels.
        reqParmSet.initAll();
        
        // Simple string arguments are just added to the request list.
        if (appParmSet.getAppArgs() != null) reqParmSet.getAppArgs().addAll(appParmSet.getAppArgs());
        if (appParmSet.getContainerArgs() != null) reqParmSet.getContainerArgs().addAll(appParmSet.getContainerArgs());
        if (appParmSet.getSchedulerOptions() != null) reqParmSet.getSchedulerOptions().addAll(appParmSet.getSchedulerOptions());
        
        // Validate that the request environment variables contains no duplicate keys.
        var reqEnvVars = reqParmSet.getEnvVariables();
        HashSet<String> origReqEnvKeys = new HashSet<String>(1 + reqEnvVars.size() * 2);
        for (var kv : reqEnvVars) {
            // Reserved keys are not allowed.
            if (kv.getKey().startsWith(TAPIS_ENV_VAR_PREFIX)) {
                String msg = MsgUtils.getMsg("JOBS_RESERVED_ENV_VAR", kv.getKey(), 
                                             TAPIS_ENV_VAR_PREFIX, "job request");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            // Duplicates are not allowed.
            if (!origReqEnvKeys.add(kv.getKey())) {
                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_ENV_VAR", "job request", kv.getKey());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
        
        // Add the environment variables from the app only if they do not already
        // exist in the request set.  The app list has already been checked for
        // duplicates and reserved names in the marshalling code.
        var appEnvVars = appParmSet.getEnvVariables();
        if (appEnvVars != null && !appEnvVars.isEmpty())
            for (var kv : appEnvVars) 
                if (!origReqEnvKeys.contains(kv.getKey())) reqParmSet.getEnvVariables().add(kv);    
        
        // Merge the archive files.
        if (appParmSet.getArchiveFilter() != null) {
            var appIncludes = appParmSet.getArchiveFilter().getIncludes();
            var appExcludes = appParmSet.getArchiveFilter().getExcludes();
            if (appIncludes != null && !appIncludes.isEmpty()) 
                reqParmSet.getArchiveFilter().getIncludes().addAll(appIncludes);
            if (appExcludes != null && !appExcludes.isEmpty()) 
                reqParmSet.getArchiveFilter().getExcludes().addAll(appExcludes);
        }
    }

    /* **************************************************************************** */
    /*                              Private Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* marshalAppArgSpecList:                                                       */
    /* ---------------------------------------------------------------------------- */
    private List<ArgSpec> marshalAppArgSpecList(
        List<edu.utexas.tacc.tapis.apps.client.gen.model.ArgSpec> appArgSpecList) 
     throws TapisImplException
    {
        // Is there anything to do?
        if (appArgSpecList == null) return null;
        
        // Field by field depth-first copy.
        List<ArgSpec> appArgs = new ArrayList<ArgSpec>(appArgSpecList.size());
        for (var appArgSpec : appArgSpecList) {
            var argSpec = new ArgSpec();
            argSpec.setArg(appArgSpec.getArg());
            if (appArgSpec.getMeta() != null) {
                var argMetaSpec = new ArgMetaSpec();
                argMetaSpec.setName(appArgSpec.getMeta().getName());
                argMetaSpec.setRequired(appArgSpec.getMeta().getRequired());
                argMetaSpec.setKv(marshalAppKvList(appArgSpec.getMeta().getKeyValuePairs()));
            }
            appArgs.add(argSpec);
        }

        return appArgs;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppAchiveFilter:                                                      */
    /* ---------------------------------------------------------------------------- */
    private IncludeExcludeFilter marshalAppAchiveFilter(
        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSetArchiveFilter appFilter)
    {
        // Is there anything to do?
        if (appFilter == null) return null;
        
        // Populate each of the filter lists.  Either or
        // both can be null or empty.
        IncludeExcludeFilter filter = new IncludeExcludeFilter();
        filter.setIncludes(appFilter.getIncludes());
        filter.setExcludes(filter.getExcludes());
        
        return filter;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppKvList:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Convenience method when there's no secondary list from systems.
     * 
     * @param appKvList apps generated kv list or null
     * @return the populated standard list or null
     * @throws TapisImplException 
     */
    private List<KeyValuePair> marshalAppKvList(
            java.util.List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> appKvList) 
      throws TapisImplException
    {return marshalAppKvList(appKvList, null);}
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppKvList:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of KeyValuePair with the generated
     * version passed by the apps and systems service.  
     * 
     * Note that we trust the apps and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param appKvList apps generated kv list or null
     * @param sysKvList systems generated kv list or null
     * @return the populated standard list or null
     * @throws TapisImplException 
     */
    private List<KeyValuePair> marshalAppKvList(
        java.util.List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> appKvList,
        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysKvList) 
     throws TapisImplException
    {
        // The kv list is optional.
        if (appKvList == null && sysKvList == null) return null;
        var kvList = new ArrayList<KeyValuePair>();
        
        // Since an app's environment variable values take precedence over
        // those set in the execution system, we use this set to track the
        // app-defined environment variable names.
        HashSet<String> appKeys = null;
        if (sysKvList != null) appKeys = new HashSet<String>();
        
        // Copy item by item from apps list.
        if (appKvList != null) {
            HashSet<String> dups = new HashSet<String>(1 + appKvList.size() * 2);
            for (var appKv : appKvList) {
                // Reserved keys are not allowed.
                if (appKv.getKey().startsWith(TAPIS_ENV_VAR_PREFIX)) {
                    String msg = MsgUtils.getMsg("JOBS_RESERVED_ENV_VAR", appKv.getKey(), 
                                                 TAPIS_ENV_VAR_PREFIX, "application");
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                // Duplicates are not allowed.
                if (!dups.add(appKv.getKey())) {
                    String msg = MsgUtils.getMsg("JOBS_DUPLICATE_ENV_VAR", "application definition", appKv.getKey());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                var kv = new KeyValuePair();
                if (appKeys != null) appKeys.add(appKv.getKey());
                kv.setKey(appKv.getKey());
                kv.setValue(appKv.getValue());
                kvList.add(kv);
            }
        }
        
        // Copy non-conflict items from systems list.
        if (sysKvList != null) {
            HashSet<String> dups = new HashSet<String>(1 + sysKvList.size() * 2);
            for (var sysKv : sysKvList) {
                // Reserved keys are not allowed.
                if (sysKv.getKey().startsWith(TAPIS_ENV_VAR_PREFIX)) {
                    String msg = MsgUtils.getMsg("JOBS_RESERVED_ENV_VAR", sysKv.getKey(), 
                                                 TAPIS_ENV_VAR_PREFIX, "execution system");
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                // Duplicates are not allowed.
                if (!dups.add(sysKv.getKey())) {
                    String msg = MsgUtils.getMsg("JOBS_DUPLICATE_ENV_VAR", "system definition", sysKv.getKey());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                // Values from the app have priority over those from the system.
                if (appKeys.contains(sysKv.getKey())) continue;
                var kv = new KeyValuePair();
                kv.setKey(sysKv.getKey());
                kv.setValue(sysKv.getValue());
                kvList.add(kv);
            }
        }
        
        return kvList;
    }
}
