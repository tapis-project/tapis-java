package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
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
     * version passed by the app service.  Note that we trust the app version to
     * conform to the schema defined in TapisDefinitions.json.
     * 
     * @param appParmSet the parameterSet retrieved from the app definition.
     * @return the populate sharedlib parameterSet object, never null
     */
    public JobParameterSet marshalAppParmSet(
        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSet appParmSet)
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
        parmSet.setEnvVariables(marshalAppKvList(appEnvVariables));
        
        // Null can be returned from the marshal method.
        var appArchiveFilter = appParmSet.getArchiveFilter();
        parmSet.setArchiveFilter(marshalAppAchiveFilter(appArchiveFilter));
        
        return parmSet;
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
    /** Populate the standard sharedlib version of KeyValueString with the generated
     * version passed by the app service.  Note that we trust the app version to
     * conform to the schema defined in TapisDefinitions.json.
     * 
     * @param appKvList app generated kv list or null
     * @return the populated standard list or null
     */
    private List<KeyValueString> marshalAppKvList(
        java.util.List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueString> appKvList)
    {
        // The kv list is optional.
        if (appKvList == null) return null;
        var kvList = new ArrayList<KeyValueString>();
        
        // Copy item by item.
        for (var appKv : appKvList) {
            var kv = new KeyValueString();
            kv.setKey(appKv.getKey());
            kv.setValue(appKv.getValue());
            kvList.add(kv);
        }
        
        return kvList;
    }
    
}
