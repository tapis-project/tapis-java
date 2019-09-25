package edu.utexas.tacc.tapis.shared.threadlocal;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public final class TapisThreadContext 
 implements Cloneable
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
    // An invalid tenant id string that indicates an uninitialized tenant id.
	public static final String INVALID_ID = "?";
	
	/* **************************************************************************** */
	/*                                    Fields                                    */
	/* **************************************************************************** */
	// The tenant and user of the current thread's request initialized to non-null.
	// Roles aren't required, so we initialize null. When present, roles take the
	// for of a comma separated lists.
	private String tenantId = INVALID_ID;
	private String user = INVALID_ID;
	private String roles = null;
	
    // The roles in list format for easy processing. 
	// Created on demand whenever roles are assigned.
    private List<String> roleList;
    
	// The execution context is set at a certain point in request processing, 
	// usually well after processing has begun.
	private TapisExecutionContext executionContext = null;
	
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
	@Override
	public TapisThreadContext clone() throws CloneNotSupportedException 
	{
	    return (TapisThreadContext) super.clone();
	}
	
	/** Validate the generic parameters required for most request processing.  This
	 * does not include execution context validation that are used only in some requests.
	 * 
	 * @return true if parameters are valid, false otherwise.
	 */
	public boolean validate()
	{
	    // Make sure required parameters have been assigned.
	    if (INVALID_ID.contentEquals(tenantId) || StringUtils.isBlank(tenantId)) return false;
	    if (INVALID_ID.contentEquals(user)     || StringUtils.isBlank(user))     return false;
	            
	    return true;
	}

    /** Validate that the execution context has been set.
     * 
     * @return true if parameters are valid, false otherwise.
     */
    public boolean validateExecutionContext(){return getExecutionContext() != null;}

	/* **************************************************************************** */
	/*                                   Accessors                                  */
	/* **************************************************************************** */
	public String getTenantId(){return tenantId;}
	public void setTenantId(String tenantId) {
		if (!StringUtils.isBlank(tenantId)) this.tenantId = tenantId;
	}
	
	public String getUser(){return user;}
	public void setUser(String user) {
	    if (!StringUtils.isBlank(user)) this.user = user;
	}

    public String getRoles() {return roles;}
    public void setRoles(String roles) {
        if (StringUtils.isBlank(roles)) return;
        this.roles = roles.replaceAll("\\s+", ""); // remove whitespace
        roleList = Arrays.asList(StringUtils.split(roles, ","));
    }
    
    public List<String> getRoleList(){return roleList;}
    
    public TapisExecutionContext getExecutionContext() {return executionContext;}
    public void setExecutionContext(TapisExecutionContext executionContext) {
        this.executionContext = executionContext;
    }
}
