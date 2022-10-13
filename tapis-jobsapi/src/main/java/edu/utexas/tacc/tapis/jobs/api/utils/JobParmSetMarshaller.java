package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppArgSpec;
import edu.utexas.tacc.tapis.apps.client.gen.model.ArgInputModeEnum;
import edu.utexas.tacc.tapis.jobs.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.JobArgSpec;
import edu.utexas.tacc.tapis.jobs.model.submit.JobParameterSet;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;

public final class JobParmSetMarshaller 
{
    /* **************************************************************************** */
    /*                                 Constants                                    */
    /* **************************************************************************** */
    // Environment variable names that start with this prefix are reserved for Tapis.
    private static final String TAPIS_ENV_VAR_PREFIX = Job.TAPIS_ENV_VAR_PREFIX;
    
    /* **************************************************************************** */
    /*                                   Enums                                      */
    /* **************************************************************************** */
    // Tags that indicate the type of arguments being processed.
    public enum ArgTypeEnum {APP_ARGS, SCHEDULER_OPTIONS, CONTAINER_ARGS}
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* mergeTapisProfileFromSystem:                                                 */
    /* ---------------------------------------------------------------------------- */
    /** This method inserts a tapis-profile scheduler option in the request 
     * schedulerOptions list if (1) the option isn't already present and (2) it was 
     * specified in the execution system definition.
     * 
     * @param schedulerOptions the request scheduler option AFTER merging with app
     *                         scheduler options.
     * @param batchSchedulerProfile the tapis-profile specified by the execution system
     */
    public void mergeTapisProfileFromSystem(List<JobArgSpec> schedulerOptions,
                                            String batchSchedulerProfile)
    {
        // Maybe there's nothing to merge.
        if (StringUtils.isBlank(batchSchedulerProfile)) return;
        final String key = Job.TAPIS_PROFILE_KEY + " ";
        
        // See if tapis-profile is already specified as a job request option.
        // The scheduler option list is never null.  If tapis-profile is found, 
        // we ignore the value defined in the system and immediately return.
        for (var opt : schedulerOptions) 
            if (opt.getArg().startsWith(key)) return;
        
        // If we get here then a tapis-profile option was not specified in
        // neither the app definition nor the job request, so the one in 
        // system wins the day.
        var spec = new JobArgSpec();
        spec.setArg(key + batchSchedulerProfile);
        spec.setDescription("The tapis-profile value set in execution system.");
        spec.setName("synthetic_tapis_profile");
        spec.setInclude(Boolean.TRUE);
        schedulerOptions.add(spec);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppParmSet:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of ParameterSet with the generated
     * data passed by the apps and systems services.  
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
        // Always create a new, uninitialized parameter set.
        var parmSet = new JobParameterSet(false);
        if (appParmSet == null) {
            // The system may define environment variables.
            parmSet.setEnvVariables(marshalAppKvList(null, sysEnv));
            return parmSet;
        }
        
        // Null can be returned from the marshal method.
        var appEnvVariables = appParmSet.getEnvVariables();
        parmSet.setEnvVariables(marshalAppKvList(appEnvVariables, sysEnv));
        
        // Null can be returned from the marshal method.
        var appArchiveFilter = appParmSet.getArchiveFilter();
        parmSet.setArchiveFilter(marshalAppAchiveFilter(appArchiveFilter));
        
        return parmSet;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeArgSpecList:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** The list of arguments from the submit request, reqList, is never null but could
     * be empty.  This list will be updated with the merged arguments completely 
     * replacing the original contents.
     * 
     * The list of arguments from the application, appList, can be null, empty or 
     * populated. 
     * 
     * @param reqList non-null submit request arguments (input/output)
     * @param appList application arguments (input only, possibly null)
     * @param argType the type of list whose args are being processed
     */
    public void mergeArgSpecList(List<JobArgSpec> reqList, List<AppArgSpec> appList,
                                 ArgTypeEnum argType)
    throws TapisImplException
    {
        // See if there's anything to do.
        int appListSize = (appList != null && appList.size() > 0) ? appList.size() : 0;
        int totalSize = reqList.size() + appListSize;
        if (totalSize == 0) return;
        
        // Create a scratch list of the maximum size and maintain proper ordering.
        var scratchList = new ArrayList<ScratchArgSpec>(totalSize);
        
        // ----------------------- App Args -----------------------
        // Maybe there's nothing to merge.
        if (appListSize > 0) {
            // Make sure there are no duplicate names among the app args.
            detectDuplicateAppArgNames(appList);
            
            // Include each qualifying app argument in the temporary list
            // preserving the original ordering.
            for (var appArg : appList) {
                // Set the input mode to the default if it's not set.
                // Args that originate from the application definition
                // always get a non-null inputMode. 
                var inputMode = appArg.getInputMode();
                if (inputMode == null) inputMode = ArgInputModeEnum.INCLUDE_ON_DEMAND;
                
                // Process the application argument.
                switch (inputMode) {
                    // These always go into the merged list.
                    case REQUIRED:
                    case FIXED:
                        scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                        
                    case INCLUDE_BY_DEFAULT:
                        if (includeArgByDefault(appArg.getName(), reqList)) 
                            scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                        
                    case INCLUDE_ON_DEMAND:
                        if (includeArgOnDemand(appArg.getName(), reqList))
                            scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                }
            }
        }        

        // --------------------- Request Args ---------------------
        // Work through the request list, overriding values on name matches
        // and adding anonymous args to the end of the scratch list.  
        if (reqList.size() > 0) {
            // Make sure there are no duplicate names among the request args.
            detectDuplicateReqArgNames(reqList);
            
            for (var reqArg : reqList) {
                // Get the name of the argument, which can be null.
                var reqName = reqArg.getName();
                
                // All ScratchArgSpecs that originate from an anonymous  
                // request argument have a null inputMode. 
                if (StringUtils.isBlank(reqName)) {
                    scratchList.add(new ScratchArgSpec(reqArg, null));
                    continue;
                }
                
                // Find the named argument in the list. 
                int scratchIndex = indexOfNamedArg(scratchList, reqName);
                if (scratchIndex < 0) {
                    // Does not override an application name.
                    scratchList.add(new ScratchArgSpec(reqArg, null));
                    continue;
                }
                
                // Merge the request values into the argument that originated 
                // from the application definition, overriding where necessary 
                // but preserving the original list order.
                mergeJobArgs(reqArg, scratchList.get(scratchIndex)._jobArg);
            }
        }
        
        // ------------- Validation and Assignment ----------------
        // The scratchList now contains all the arguments from both the application
        // and the job request, with the application arguments listed first in their
        // original order, followed by the job request arguments in their original
        // order.  When a request argument overrode an application argument, the
        // values of the former were written to the latter and application argument
        // ordering was preserved.
        validateScratchList(scratchList, argType);
        
        // Save the contents of the scratchList to the reqList, completely replacing
        // the original reqList content.
        assignReqList(scratchList, reqList);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeNonArgParms:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Merge each of the non-JobArgSpec components of the application/system parameter 
     * set into the request parameter set.  
     * 
     * @param reqParmSet non-null parameters from job request, used for input and output
     * @param appParmSet non-null parameters from application and system, input only
     * @throws TapisImplException
     */
    public void mergeNonArgParms(JobParameterSet reqParmSet, JobParameterSet appParmSet) 
     throws TapisImplException
    {
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
        
        // Merge the archive files.  The elements of the includes and excludes lists can
        // be globs or regexes.  The two are distinguished by prefixing regexes with "REGEX:"
        // whereas globs are written as they would appear on a command line.
        if (appParmSet.getArchiveFilter() != null) {
            var appIncludes = appParmSet.getArchiveFilter().getIncludes();
            var appExcludes = appParmSet.getArchiveFilter().getExcludes();
            if (appIncludes != null && !appIncludes.isEmpty()) 
                reqParmSet.getArchiveFilter().getIncludes().addAll(appIncludes);
            if (appExcludes != null && !appExcludes.isEmpty()) 
                reqParmSet.getArchiveFilter().getExcludes().addAll(appExcludes);
            
            // Assign the launch file inclusion flag.
            if (reqParmSet.getArchiveFilter().getIncludeLaunchFiles() == null)
                reqParmSet.getArchiveFilter().setIncludeLaunchFiles(appParmSet.getArchiveFilter().getIncludeLaunchFiles());
            if (reqParmSet.getArchiveFilter().getIncludeLaunchFiles() == null)
                reqParmSet.getArchiveFilter().setIncludeLaunchFiles(Boolean.TRUE);
        }
    }

    /* **************************************************************************** */
    /*                              Private Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* makeScratchArg:                                                              */
    /* ---------------------------------------------------------------------------- */
    private ScratchArgSpec makeScratchArg(AppArgSpec appArg, ArgInputModeEnum inputMode)
    {
        return new ScratchArgSpec(convertToJobArgSpec(appArg), inputMode);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* convertToJobArgSpec:                                                         */
    /* ---------------------------------------------------------------------------- */
    private JobArgSpec convertToJobArgSpec(AppArgSpec appArg)
    {
        var jobArg = new JobArgSpec();
        jobArg.setName(appArg.getName());
        jobArg.setDescription(appArg.getDescription());
        jobArg.setArg(appArg.getArg());
//        jobArg.setMeta(appArg.getMeta());  // TODO: uncomment **************
        return jobArg;
    }
    
    // ============================================
    // Truth Table for App Arg Inclusion
    //  
    //  AppArgSpec          JobArgSpec  Meaning
    //  inputMode           include 
    //  -------------------------------------------
    //  INCLUDE_ON_DEMAND   True        include arg
    //  INCLUDE_ON_DEMAND   False       exclude arg
    //  INCLUDE_ON_DEMAND   undefined   include arg
    //  INCLUDE_BY_DEFAULT  True        include arg
    //  INCLUDE_BY_DEFAULT  False       exclude arg
    //  INCLUDE_BY_DEFAULT  undefined   include arg
    // ============================================
    
    /* ---------------------------------------------------------------------------- */
    /* includeArgByDefault:                                                         */
    /* ---------------------------------------------------------------------------- */
    private boolean includeArgByDefault(String argName, List<JobArgSpec> reqList)
    {
        // See if the include-by-default appArg has been 
        // explicitly excluded in the request.
        for (var reqArg : reqList) {
            if (!argName.equals(reqArg.getName())) continue;
            if (reqArg.getInclude() == null || reqArg.getInclude()) return true;
              else return false;
        }
        
        // If the appArg is not referenced in a request arg,
        // the default action is to respect the app definition
        // and include it by default.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* includeArgOnDemand:                                                          */
    /* ---------------------------------------------------------------------------- */
    private boolean includeArgOnDemand(String argName, List<JobArgSpec> reqList)
    {
        // See if the include-on-demand appArg should be included in the 
        // request by either being simply referenced or explicitly included.
        for (var reqArg : reqList) {
            if (!argName.equals(reqArg.getName())) continue;
            if (reqArg.getInclude() == null || reqArg.getInclude()) return true;
              else return false;
        }
        
        // If the appArg is not referenced in a request arg,
        // the default action is to respect the app definition
        // and not include it.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectDuplicateAppArgNames:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Validate that there each argument in list from an application definition has
     * a unique name.
     * 
     * @param appList application definition arg list
     * @throws TapisImplException when duplicates are detected
     */
    private void detectDuplicateAppArgNames(List<AppArgSpec> appList) throws TapisImplException
    {
        var names = new HashSet<String>(2*appList.size()+1);
        for (var appArg : appList)
            if (!names.add(appArg.getName())) {
                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_NAMED_ARG", 
                                             "application definition", appArg.getName());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectDuplicateReqArgNames:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Validate that there each argument in list from a job submission request has
     * a unique name among those that provide names.
     * 
     * @param reqList job request definition arg list
     * @throws TapisImplException when duplicates are detected
     */
    private void detectDuplicateReqArgNames(List<JobArgSpec> reqList) throws TapisImplException
    {
        var names = new HashSet<String>(2*reqList.size()+1);
        for (var reqArg : reqList) {
            var name = reqArg.getName();
            if (StringUtils.isBlank(name)) continue;
            if (!names.add(name)) {
                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_NAMED_ARG", "job request", name);
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* indexOfNamedArg:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Find the argument in the list with the specified name.
     * 
     * @param scratchList non-null scratch list
     * @param name non-empty search name
     * @return the index of the matching element or -1 for no match
     */
    private int indexOfNamedArg(List<ScratchArgSpec> scratchList, String name)
    {
        // See if the named argument already exists in the list.
        for (int i = 0; i < scratchList.size(); i++) {
            var curName = scratchList.get(i)._jobArg.getName();
            if (StringUtils.isBlank(curName)) continue;
            if (curName.equals(name)) return i;
        }
        
        // No match.
        return -1;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeJobArgs:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** The sourceArg is a reqArg and the targetArg is an appArg that has already been
     * converted into a JobArgSpec.  The targetArg is already in the result list and
     * this method performs in-place replacement of appArg values with those from
     * the reqArg.
     * 
     * @param sourceArg reqArg
     * @param targetArg a converted appArg that we may modify
     */
    private void mergeJobArgs(JobArgSpec sourceArg, JobArgSpec targetArg)
    {
        // The request flag always assigns the target flag, which is always null.
        targetArg.setInclude(sourceArg.getInclude());
        
        // Conditional replacement.
        if (!StringUtils.isBlank(sourceArg.getArg())) 
            targetArg.setArg(sourceArg.getArg());
        if (!StringUtils.isBlank(sourceArg.getMeta()))
            targetArg.setMeta(sourceArg.getMeta());
        
        // Append a non-empty source description to an existing target description.
        // Otherwise, just assign the target description the non-empty source description.
        if (!StringUtils.isBlank(sourceArg.getDescription()))
            if (StringUtils.isBlank(targetArg.getDescription()))
                targetArg.setDescription(sourceArg.getDescription());
            else 
                targetArg.setDescription(
                    targetArg.getDescription() + "\n\n" + sourceArg.getDescription());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateScratchList:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void validateScratchList(List<ScratchArgSpec> scratchList, ArgTypeEnum argType)
     throws TapisImplException
    {
        // Make sure all arguments are either complete or able to be removed.  
        // Incomplete arguments that originated in the app are removable if their
        // inputMode is INCLUDE_BY_DEFAULT.  All other incomplete arguments cause
        // an error.  A null input mode indicates the argument originated from 
        // the job request.
        var it = scratchList.listIterator();
        while (it.hasNext()) {
            var elem = it.next();
            if (StringUtils.isBlank(elem._jobArg.getArg()))
                if (elem._inputMode == ArgInputModeEnum.INCLUDE_BY_DEFAULT) {
                    it.remove();
                }
                else {
                    String msg = MsgUtils.getMsg("JOBS_MISSING_ARG", elem._jobArg.getName(), argType);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignReqList:                                                               */
    /* ---------------------------------------------------------------------------- */
    private void assignReqList(List<ScratchArgSpec> scratchList, List<JobArgSpec> reqList)
    {
        // Always clear the request list.
        reqList.clear();
        
        // Assign each of the arguments from the scratch list.
        for (var elem : scratchList) reqList.add(elem._jobArg);
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
        // both input lists can be null or empty; we 
        // initialize null lists after all assignments.
        IncludeExcludeFilter filter = new IncludeExcludeFilter(false);
        filter.setIncludes(appFilter.getIncludes());
        filter.setExcludes(appFilter.getExcludes());
        filter.setIncludeLaunchFiles(appFilter.getIncludeLaunchFiles());
        filter.initAll(); // assign empty lists if null
        
        return filter;
    }
    
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
    
    /* **************************************************************************** */
    /*                            ScratchArgSpec Class                              */
    /* **************************************************************************** */
    // Simple record for internal use only.
    private static final class ScratchArgSpec
    {
        private ArgInputModeEnum _inputMode;
        private JobArgSpec       _jobArg;
        
        private ScratchArgSpec(JobArgSpec jobArg, ArgInputModeEnum inputMode) 
        {_jobArg = jobArg; _inputMode = inputMode;}
    }
}
