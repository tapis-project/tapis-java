package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobRecovery
{
    private int     id;
    private String  tenantId;
    private String  conditionCode;
    private String  testerType;
    private String  testerParms;
    private String  policyType;
    private String  policyParms;
    private int     numAttempts;
    private Instant nextAttempt;
    private Instant created;
    private Instant lastUpdated;
    private String  testerHash;

    @Override
    public String toString() {return TapisUtils.toString(this);}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getConditionCode() {
		return conditionCode;
	}

	public void setConditionCode(String conditionCode) {
		this.conditionCode = conditionCode;
	}

	public String getTesterType() {
		return testerType;
	}

	public void setTesterType(String testerType) {
		this.testerType = testerType;
	}

	public String getTesterParms() {
		return testerParms;
	}

	public void setTesterParms(String testerParms) {
		this.testerParms = testerParms;
	}

	public String getPolicyType() {
		return policyType;
	}

	public void setPolicyType(String policyType) {
		this.policyType = policyType;
	}

	public String getPolicyParms() {
		return policyParms;
	}

	public void setPolicyParms(String policyParms) {
		this.policyParms = policyParms;
	}

	public int getNumAttempts() {
		return numAttempts;
	}

	public void setNumAttempts(int numAttempts) {
		this.numAttempts = numAttempts;
	}

	public Instant getNextAttempt() {
		return nextAttempt;
	}

	public void setNextAttempt(Instant nextAttempt) {
		this.nextAttempt = nextAttempt;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	public Instant getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Instant lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getTesterHash() {
		return testerHash;
	}

	public void setTesterHash(String testerHash) {
		this.testerHash = testerHash;
	}
}
