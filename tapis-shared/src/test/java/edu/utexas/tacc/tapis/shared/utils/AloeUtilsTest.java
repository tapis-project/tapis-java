package edu.utexas.tacc.tapis.shared.utils;

import java.io.FileNotFoundException;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.AloeException;
import edu.utexas.tacc.tapis.shared.exceptions.AloeJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.AloeJSONException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.AloeDBConnectionException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.AloeRuntimeException;
import edu.utexas.tacc.tapis.shared.utils.AloeUtils;

@Test(groups={"unit"})
public class AloeUtilsTest 
{
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getLocalHostnameTest:                                                        */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void getLocalHostnameTest()
    {
        String hostname = AloeUtils.getLocalHostname();
        System.out.println("hostname: " + hostname);
        Assert.assertNotNull(hostname, "Expected non-null hostname.");
        Assert.assertNotEquals(hostname, "", "Expected non-empty hostname.");
        Assert.assertNotEquals(hostname, "localhost", "Expected a hostname other than localhost.");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* aloeifyTest:                                                                 */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void aloeifyTest()
    {
        // Wrapped exception.
        AloeException wrappedException;
        String originalMsg = "original msg";
        
        // ========= null new msg =========
        // The new message is contained in the returned exception.
        String newMsg = "new msg";
        
        // ----- AloeException
        AloeException e = new AloeException(originalMsg);
        wrappedException = AloeUtils.aloeify(e, newMsg);
        Assert.assertEquals(wrappedException.getClass(), e.getClass(), 
                            "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), newMsg,
                            "Unexpected message returned.");
        
        // ----- AloeJDBCException
        e = new AloeJDBCException(originalMsg);
        wrappedException = AloeUtils.aloeify(e, newMsg);
        Assert.assertEquals(wrappedException.getClass(), e.getClass(), 
                            "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), newMsg,
                            "Unexpected message returned.");
        
        // ----- AloeRuntimeException
        AloeRuntimeException erun = new AloeRuntimeException(originalMsg);
        wrappedException = AloeUtils.aloeify(erun, newMsg);
        Assert.assertEquals(wrappedException.getClass(), AloeException.class, 
                           "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), newMsg,
                            "Unexpected message returned.");
     
        // ========= null new msg =========
        // The original message is preserved in the returned exception.
        newMsg = null;
        
        // ----- AloeException
        e = new AloeException(originalMsg);
        wrappedException = AloeUtils.aloeify(e, newMsg);
        Assert.assertEquals(wrappedException.getClass(), e.getClass(), 
                            "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), originalMsg,
                            "Unexpected message returned.");
        
        // ----- AloeJDBCException
        e = new AloeJDBCException(originalMsg);
        wrappedException = AloeUtils.aloeify(e, newMsg);
        Assert.assertEquals(wrappedException.getClass(), e.getClass(), 
                            "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), originalMsg,
                            "Unexpected message returned.");
        
        // ----- AloeRuntimeException
        erun = new AloeRuntimeException(originalMsg);
        wrappedException = AloeUtils.aloeify(erun, newMsg);
        Assert.assertEquals(wrappedException.getClass(), AloeException.class, 
                           "Unexpected exception type returned: " + wrappedException.getClass().getSimpleName());
        Assert.assertEquals(wrappedException.getMessage(), originalMsg,
                            "Unexpected message returned.");
    }

    /* ---------------------------------------------------------------------------- */
    /* aloeifyTest:                                                                 */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void findInChainTest()
    {
        // Create an exception chain.
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("x");
        AloeDBConnectionException aloeDBConnectionException = new AloeDBConnectionException("x", illegalArgumentException);
        AloeException aloeException = new AloeException("x", aloeDBConnectionException);
        
        // Inspect the chain using declared types.
        Exception e = AloeUtils.findInChain(aloeException, AloeDBConnectionException.class);
        Assert.assertNotNull(e, "AloeDBConnectionException not found!");
        e = AloeUtils.findInChain(aloeException, AloeException.class);
        Assert.assertNotNull(e, "AloeException not found!");
        e = AloeUtils.findInChain(aloeException, IllegalArgumentException.class);
        Assert.assertNotNull(e, "IllegalArgumentException not found!");
        
        // More inspection.
        e = AloeUtils.findInChain(aloeException, Exception.class);
        Assert.assertNotNull(e, "Exception not found!");
        e = AloeUtils.findInChain(aloeException, FileNotFoundException.class);
        Assert.assertNull(e, "FileNotFoundException should not be found!");
        e = AloeUtils.findInChain(aloeException, FileNotFoundException.class, IllegalArgumentException.class);
        Assert.assertNotNull(e, "IllegalArgumentException not found!");
        e = AloeUtils.findInChain(aloeException, AloeJSONException.class);
        Assert.assertNull(e, "AloeJSONException should not be found!");
        
        // Start in middle of chain.
        e = AloeUtils.findInChain(aloeDBConnectionException, AloeException.class);
        Assert.assertNotNull(e, "AloeException not found!");
        e = AloeUtils.findInChain(aloeDBConnectionException, IllegalArgumentException.class);
        Assert.assertNotNull(e, "IllegalArgumentException not found!");
   }
}
