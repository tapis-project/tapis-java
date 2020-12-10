package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import edu.utexas.tacc.tapis.shared.model.ArgMetaSpec;
import edu.utexas.tacc.tapis.shared.model.ArgSpec;
import edu.utexas.tacc.tapis.shared.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.shared.model.JobParameterSet;
import edu.utexas.tacc.tapis.shared.model.KeyValueString;

public final class JobParmSetMarshaller 
{
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
     */
    public JobParameterSet marshalAppParmSet(
        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSet appParmSet,
        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueString> sysEnv)
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
    public JobParameterSet mergeParmSets(JobParameterSet reqParmSet,
                                         JobParameterSet appParmSet)
    {
        return reqParmSet;
    }

    /* **************************************************************************** */
    /*                              Private Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* marshalAppArgSpecList:                                                       */
    /* ---------------------------------------------------------------------------- */
    private List<ArgSpec> marshalAppArgSpecList(
        List<edu.utexas.tacc.tapis.apps.client.gen.model.ArgSpec> appArgSpecList)
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
                argMetaSpec.setKv(marshalAppKvList(appArgSpec.getMeta().getKv()));
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
     */
    private List<KeyValueString> marshalAppKvList(
            java.util.List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueString> appKvList)
    {return marshalAppKvList(appKvList, null);}
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppKvList:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of KeyValueString with the generated
     * version passed by the apps and systems service.  
     * 
     * Note that we trust the apps and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param appKvList apps generated kv list or null
     * @param sysKvList systems generated kv list or null
     * @return the populated standard list or null
     */
    private List<KeyValueString> marshalAppKvList(
        java.util.List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueString> appKvList,
        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueString> sysKvList)
    {
        // The kv list is optional.
        if (appKvList == null && sysKvList == null) return null;
        var kvList = new ArrayList<KeyValueString>();
        
        // Since an app's environment variable values take precedence over
        // those set in the execution system, we use this set to track the
        // app-defined environment variable names.
        HashSet<String> appKeys = null;
        if (sysKvList != null) appKeys = new HashSet<String>();
        
        // Copy item by item from apps list.
        if (appKvList != null)
            for (var appKv : appKvList) {
                var kv = new KeyValueString();
                if (appKeys != null) appKeys.add(appKv.getKey());
                kv.setKey(appKv.getKey());
                kv.setValue(appKv.getValue());
                kvList.add(kv);
            }
        
        // Copy non-conflict items from systems list.
        if (sysKvList != null)
            for (var sysKv : sysKvList) {
                if (appKeys.contains(sysKv.getKey())) continue;
                var kv = new KeyValueString();
                kv.setKey(sysKv.getKey());
                kv.setValue(sysKv.getValue());
                kvList.add(kv);
            }
        
        return kvList;
    }
    
}
