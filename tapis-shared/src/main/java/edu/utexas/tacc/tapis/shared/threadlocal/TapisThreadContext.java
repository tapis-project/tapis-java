package edu.utexas.tacc.tapis.shared.threadlocal;

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
    /*                                     Enums                                    */
    /* **************************************************************************** */
	public enum AccountType {user, service}
	
	/* **************************************************************************** */
	/*                                    Fields                                    */
	/* **************************************************************************** */
	// The tenant and user of the current thread's request initialized to non-null.
	// Account type also cannot be null.  The delegator subject is either null when
	// no delegation has occurred or in the 'user@tenant' format when there is 
	// delegation.
	private String tenantId = INVALID_ID;
	private String user = INVALID_ID;
	private AccountType accountType;
    private String delegatorSubject = null; 
	
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
	    if (accountType == null) return false;
	            
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

    public AccountType getAccountType() {return accountType;}
    public void setAccountType(AccountType accountType) {this.accountType = accountType;}
	
    public TapisExecutionContext getExecutionContext() {return executionContext;}
    public void setExecutionContext(TapisExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    public String getDelegatorSubject() {
        return delegatorSubject;
    }
    public void setDelegatorSubject(String delegatorSubject) {
        this.delegatorSubject = delegatorSubject;
    }
}
