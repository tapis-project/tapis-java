package edu.utexas.tacc.tapis.shared.threadlocal;

import javax.sql.DataSource;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface TapisExecutionContext 
{
    /** Retrieve the tenant corresponding to the current tenant id.
     * @return a edu.utexas.tacc.tapis.tenants.model.Tenant object
     */
    Object getTenant() throws TapisException;
    
    /** Retrieve the tenant's base url without requiring the caller
     * to have access to the Tenant class.
     * @return the current tenant's base url as a string
     */
    String getTenantBaseUrl() throws TapisException;
    
    /** Get the source for connections to our database.
     * @return the database connection source
     */
    DataSource getDataSource() throws TapisException;
}
