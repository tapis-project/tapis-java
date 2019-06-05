package edu.utexas.tacc.tapis.shared;

/** This class is a repository for constants used throughout Tapis.
 * 
 * @author rcardone
 *
 */
public class TapisConstants 
{
	// Service names used to identify service code externally.
	public static final String SERVICE_NAME_JOBS = "db.migration.db.migration.jobs";
	public static final String SERVICE_NAME_SAMPLE = "sample";
	public static final String SERVICE_NAME_UUIDS = "uuid";

	// Thread local logging identifier.
	public static final String MDC_ID_KEY = "UNIQUE_ID";
	
	// General query parameters that are used on various endpoints.
	public static final String QUERY_PARM_TEST_TENANT = "testTenant";
	public static final String QUERY_PARM_TEST_USER   = "testUser";
	
	// Custom JAXRS filter priorities used to order filter execution.
	public static final int JAXRS_FILTER_PRIORITY_BEFORE_AUTHENTICATION  = 900;
	public static final int JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION   = 1100;
	public static final int JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION_2 = 1200;
	
	// This URL comes from Agave with no explanation and the host address does 
	// not resolve using ping, dig or ssh.  Since this URL is hardcoded in
	// db.migration.db.migration.jobs and systems, it is used in the develop and staging environments as
	// well as in production.  We don't know why this exists.  
    public static final String MAGIC_TRANSFER_URL = "https://workers.prod.tacc.cloud/";
    
	// Convenience definition.
	public static final String EMPTY_JSON = "{}";
	
    // Specified in a Settings file in the orginal Agave code.
	public static final String PUBLIC_USER_USERNAME = "public";
	public static final String WORLD_USER_USERNAME  = "world";
	
	// ----------- URL Components -----------
	public static final String API_VERSION = "v3";
	public static final String APPS_SERVICE          = "apps/" + API_VERSION + "/";
	public static final String JOBS_SERVICE          = "db.migration.db.migration.jobs/" + API_VERSION + "/";
	public static final String FILES_SERVICE         = "files/" + API_VERSION + "/";
    public static final String POSTIT_SERVICE        = "postits/" + API_VERSION + "/";
	public static final String META_SERVICE          = "meta/" + API_VERSION + "/";
	public static final String NOTIFICATIONS_SERVICE = "notifications/" + API_VERSION + "/";

	public static final String META_SCHEMA_SERVICE  = "schemas/";
	public static final String SYSTEMS_SERVICE  = "systems/" + API_VERSION + "/";
    public static final String TRANSFERS_SERVICE  = "transfers/" + API_VERSION + "/";

	        
	// Placeholder URLs used before tenant URL substitution takes place.
	public static final String DUMMY_URL_PREFIX   = "none://dummy.nowhere/";
	public static final String DUMMY_URL_JOBS     = DUMMY_URL_PREFIX + JOBS_SERVICE;
	public static final String DUMMY_URL_FILES    = DUMMY_URL_PREFIX + FILES_SERVICE;
	public static final String DUMMY_URL_META     = DUMMY_URL_PREFIX + META_SERVICE;
	public static final String DUMMY_URL_SYSTEMS  = DUMMY_URL_PREFIX + SYSTEMS_SERVICE;
}
