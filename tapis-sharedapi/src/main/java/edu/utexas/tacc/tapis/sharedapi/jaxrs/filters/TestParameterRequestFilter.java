package edu.utexas.tacc.tapis.sharedapi.jaxrs.filters;

import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext.AccountType;

/** Determine if test parameter usage is enabled.  This filter runs after the
 * JWT authentication filter and can change some values set by that filter.
 * Values can be passed in the following HTTP headers and are saved in their
 * corresponding threadlocal field:
 * 
 *  1. X-Tapis-Test-Tenant
 *  2. X-Tapis-Test-User
 *  3. X-Tapis-Test-Account-Type
 *  4. X-Tapis-Test-Delegation-Sub
 * 
 * @author rcardone
 */
@Provider
@Priority(TapisConstants.JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION)
public class TestParameterRequestFilter 
 implements ContainerRequestFilter
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(TestParameterRequestFilter.class);
    
    // The header keys that we recognize.
    private static final String TEST_TENANT         = "X-Tapis-Test-Tenant";
    private static final String TEST_USER           = "X-Tapis-Test-User";
    private static final String TEST_ACCOUNT_TYPE   = "X-Tapis-Test-Account-Type";
    private static final String TEST_DELEGATION_SUB = "X-Tapis-Test-Delegation-Sub";
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* filter:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void filter(ContainerRequestContext requestContext) 
    {
        // Tracing.
        if (_log.isTraceEnabled())
            _log.trace("Executing JAX-RX request filter: " + this.getClass().getSimpleName() + ".");
        
        // Determine if we are ignoring or respecting test parameters.
        if (!TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS)) return;
        
        // Initialize all test parameter values.
        String tenantId = null;
        String user = null;
        AccountType accountType = null;
        String delegationSub = null;
        
        // Iterate through all headers looking for the tapis test keys.
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        for (Entry<String, List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase(TEST_TENANT)) {
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty())
                    tenantId = values.get(0);
            }
            else if (key.equalsIgnoreCase(TEST_USER)) {
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty())
                    user = values.get(0);
            }
            else if (key.equalsIgnoreCase(TEST_ACCOUNT_TYPE)) {
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty()) {
                    try {accountType = AccountType.valueOf(values.get(0));}
                    catch (Exception e) {
                        String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", TEST_ACCOUNT_TYPE,
                                                     values.get(0));
                        _log.error(msg, e);
                        requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
                        return;
                    }
                }
            }
            else if (key.equalsIgnoreCase(TEST_DELEGATION_SUB)) {
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty())
                    delegationSub = values.get(0);
            }
        }

        // Assign non-null test values to thread-local variables.
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        if (!StringUtils.isBlank(tenantId)) threadContext.setTenantId(tenantId);
        if (!StringUtils.isBlank(user)) threadContext.setUser(user);
        if (accountType != null) threadContext.setAccountType(accountType);
        if (!StringUtils.isBlank(delegationSub)) threadContext.setDelegatorSubject(delegationSub);
    }
}
