package edu.utexas.tacc.tapis.shared.parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;

import java.util.HashMap;
import java.util.Map;


@Test(groups={"unit"})
public class TapisEnvTest
{
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(TapisEnvTest.class);
    // Save copy of existing env for restoring later
    private Map<String, String> oldEnv = new HashMap<>(System.getenv());
    private static final Integer INT_TEST_VAL = 8080;
    private static final Long LONG_TEST_VAL = 9090L;
    private static final Double DBL_TEST_VAL = 1.1d;

    /*
     * Modify env to seed variables used in testing
     */
    @BeforeClass
    public void setUp() throws Exception
    {
        // Create new copy to modify
        Map<String, String> newEnv = new HashMap<>(oldEnv);
        // Add env vars to be used in testing
        //   tapis.envonly.allow.test.query.parms=true
        //   TAPIS_ENVONLY_JWT_OPTIONAL=true
        //   tapis.envonly.keystore.password=value1
        //   TAPIS_ENVONLY_KEYSTORE_PASSWORD=value2
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS.getEnvName(), "true");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL.name(), "true");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD.getEnvName(), "value1");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD.name(), "value2");
        newEnv.put(TapisEnv.EnvVar.TAPIS_QUEUE_PORT.name(), INT_TEST_VAL.toString());
        newEnv.put(TapisEnv.EnvVar.TAPIS_SMTP_PORT.name(), LONG_TEST_VAL.toString());
        newEnv.put(TapisEnv.EnvVar.TAPIS_SMTP_PASSWORD.name(), DBL_TEST_VAL.toString());
        newEnv.put(TapisEnv.EnvVar.TAPIS_REQUEST_LOGGING_FILTER_PREFIXES.getEnvName(), "a,bc,def,/g/h/i");
        newEnv.put(TapisEnv.EnvVar.TAPIS_MAIL_PROVIDER.getEnvName(), "");

        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_LOG_SECURITY_INFO.getEnvName(), "true");

        // Update env
        ParmTestUtils.setEnv(newEnv);
        ParmTestUtils.printTapisEnvVars();
    }

    @AfterClass
    public void tearDown() throws Exception
    {
        // Restore env
        ParmTestUtils.setEnv(oldEnv);
    }

    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */

    /* ----------------------------------------------------------------------------
     * Check that env var value correctly retrieved for
     *    env var of form tapis.envonly.allow.test.query.parms
     *    env var of form TAPIS_ENVONLY_JWT_OPTIONAL
     *    env var of form tapis.envonly... has precedence over TAPIS_ENVONLY...
     * ---------------------------------------------------------------------------- */
    @Test
    public void testGet()
    {
        // Check EnvVar.getEnvName
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS;
        String envVal = TapisEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was empty for: " + envVar);
        Assert.assertEquals(envVal, "true");

        // Check EnvVar.name
        envVar = TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL;
        envVal = TapisEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was empty for: " + envVar);
        Assert.assertEquals(envVal, "true");

        // Check that EnvVar.getEnvName has precedence over EnvVar.name
        envVar = TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD;
        envVal = TapisEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was null for: " + envVar);
        Assert.assertEquals(envVal, "value1");
    }

    @Test
    public void testGetBoolean()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS;
        boolean envVal = TapisEnv.getBoolean(envVar);
        Assert.assertTrue(envVal);
    }

    @Test
    public void testGetInteger()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_QUEUE_PORT;
        Integer envVal = TapisEnv.getInteger(envVar);
        Assert.assertEquals(envVal, INT_TEST_VAL);
    }

    @Test
    public void testGetLong()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_SMTP_PORT;
        Long envVal = TapisEnv.getLong(envVar);
        Assert.assertEquals(envVal, LONG_TEST_VAL);
    }

    @Test
    public void testGetDouble()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_SMTP_PASSWORD;
        Double envVal = TapisEnv.getDouble(envVar);
        Assert.assertEquals(envVal, DBL_TEST_VAL);
    }

    @Test
    public void testInEnvVarList()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_REQUEST_LOGGING_FILTER_PREFIXES;
        String member = "bc";
        // Check that false is returned if either parameter is null
        Assert.assertFalse(TapisEnv.inEnvVarList(null,null));
        Assert.assertFalse(TapisEnv.inEnvVarList(envVar,null));
        Assert.assertFalse(TapisEnv.inEnvVarList(null,member));
        // Check that true is returned if value is in list
        Assert.assertTrue(TapisEnv.inEnvVarList(envVar, member));
        // Check that false is returned if value is not in list
        Assert.assertFalse(TapisEnv.inEnvVarList(envVar, "zzz"));
        // Check that false is returned if env var value is empty
        envVar = TapisEnv.EnvVar.TAPIS_MAIL_PROVIDER;
        Assert.assertFalse(TapisEnv.inEnvVarList(envVar, "a"));
    }

    @Test(enabled=true)
    public void testInEnvVarListPrefix()
    {
        TapisEnv.EnvVar envVar = TapisEnv.EnvVar.TAPIS_REQUEST_LOGGING_FILTER_PREFIXES;
        String name = "/g";
        // Check that false is returned if either parameter is null
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(null,null));
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(envVar,null));
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(null,name));
        // Check that true is returned if value is a prefix in the list
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(envVar, name));
        // Check that true is returned if value is a prefix in the list
        Assert.assertTrue(TapisEnv.inEnvVarListPrefix(envVar, "/g/h/i/j/k"));
        // Check that true is returned if value is a prefix in the list
        Assert.assertTrue(TapisEnv.inEnvVarListPrefix(envVar, "a33"));
        // Check that true is returned if value is a prefix in the list
        Assert.assertTrue(TapisEnv.inEnvVarListPrefix(envVar, "bc/y"));
        // Check that true is returned if value is a prefix in the list
        Assert.assertTrue(TapisEnv.inEnvVarListPrefix(envVar, "def"));
        // Check that false is returned if value is not in list
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(envVar, "/zzz"));
        // Check that false is returned if env var value is empty
        envVar = TapisEnv.EnvVar.TAPIS_MAIL_PROVIDER;
        Assert.assertFalse(TapisEnv.inEnvVarListPrefix(envVar, "a"));
    }

    @Test
    public void testGetLogSecurityInfo()
    {
        boolean envVal = TapisEnv.getLogSecurityInfo();
        Assert.assertTrue(envVal);
    }
}