package edu.utexas.tacc.tapis.shared.parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.parameters.AloeEnv;

import java.util.HashMap;
import java.util.Map;


@Test(groups={"unit"})
public class AloeEnvTest
{
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AloeEnvTest.class);
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
        //   aloe.envonly.allow.test.query.parms=true
        //   ALOE_ENVONLY_JWT_OPTIONAL=true
        //   aloe.envonly.keystore.password=value1
        //   ALOE_ENVONLY_KEYSTORE_PASSWORD=value2
        newEnv.put(AloeEnv.EnvVar.ALOE_ENVONLY_ALLOW_TEST_QUERY_PARMS.getEnvName(), "true");
        newEnv.put(AloeEnv.EnvVar.ALOE_ENVONLY_JWT_OPTIONAL.name(), "true");
        newEnv.put(AloeEnv.EnvVar.ALOE_ENVONLY_KEYSTORE_PASSWORD.getEnvName(), "value1");
        newEnv.put(AloeEnv.EnvVar.ALOE_ENVONLY_KEYSTORE_PASSWORD.name(), "value2");
        newEnv.put(AloeEnv.EnvVar.ALOE_QUEUE_PORT.name(), INT_TEST_VAL.toString());
        newEnv.put(AloeEnv.EnvVar.ALOE_SMTP_PORT.name(), LONG_TEST_VAL.toString());
        newEnv.put(AloeEnv.EnvVar.ALOE_SMTP_PASSWORD.name(), DBL_TEST_VAL.toString());
        newEnv.put(AloeEnv.EnvVar.ALOE_REQUEST_LOGGING_FILTER_PREFIXES.getEnvName(), "a,bc,def,/g/h/i");
        newEnv.put(AloeEnv.EnvVar.ALOE_MAIL_PROVIDER.getEnvName(), "");

        newEnv.put(AloeEnv.EnvVar.ALOE_ENVONLY_LOG_SECURITY_INFO.getEnvName(), "true");

        // Update env
        ParmTestUtils.setEnv(newEnv);
        ParmTestUtils.printAloeEnvVars();
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
     *    env var of form aloe.envonly.allow.test.query.parms
     *    env var of form ALOE_ENVONLY_JWT_OPTIONAL
     *    env var of form aloe.envonly... has precedence over ALOE_ENVONLY...
     * ---------------------------------------------------------------------------- */
    @Test
    public void testGet()
    {
        // Check EnvVar.getEnvName
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_ENVONLY_ALLOW_TEST_QUERY_PARMS;
        String envVal = AloeEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was empty for: " + envVar);
        Assert.assertEquals(envVal, "true");

        // Check EnvVar.name
        envVar = AloeEnv.EnvVar.ALOE_ENVONLY_JWT_OPTIONAL;
        envVal = AloeEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was empty for: " + envVar);
        Assert.assertEquals(envVal, "true");

        // Check that EnvVar.getEnvName has precedence over EnvVar.name
        envVar = AloeEnv.EnvVar.ALOE_ENVONLY_KEYSTORE_PASSWORD;
        envVal = AloeEnv.get(envVar);
        Assert.assertNotNull(envVal, "Property from environment was null for: " + envVar);
        Assert.assertFalse(envVal.isEmpty(), "Property from environment was null for: " + envVar);
        Assert.assertEquals(envVal, "value1");
    }

    @Test
    public void testGetBoolean()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_ENVONLY_ALLOW_TEST_QUERY_PARMS;
        boolean envVal = AloeEnv.getBoolean(envVar);
        Assert.assertTrue(envVal);
    }

    @Test
    public void testGetInteger()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_QUEUE_PORT;
        Integer envVal = AloeEnv.getInteger(envVar);
        Assert.assertEquals(envVal, INT_TEST_VAL);
    }

    @Test
    public void testGetLong()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_SMTP_PORT;
        Long envVal = AloeEnv.getLong(envVar);
        Assert.assertEquals(envVal, LONG_TEST_VAL);
    }

    @Test
    public void testGetDouble()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_SMTP_PASSWORD;
        Double envVal = AloeEnv.getDouble(envVar);
        Assert.assertEquals(envVal, DBL_TEST_VAL);
    }

    @Test
    public void testInEnvVarList()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_REQUEST_LOGGING_FILTER_PREFIXES;
        String member = "bc";
        // Check that false is returned if either parameter is null
        Assert.assertFalse(AloeEnv.inEnvVarList(null,null));
        Assert.assertFalse(AloeEnv.inEnvVarList(envVar,null));
        Assert.assertFalse(AloeEnv.inEnvVarList(null,member));
        // Check that true is returned if value is in list
        Assert.assertTrue(AloeEnv.inEnvVarList(envVar, member));
        // Check that false is returned if value is not in list
        Assert.assertFalse(AloeEnv.inEnvVarList(envVar, "zzz"));
        // Check that false is returned if env var value is empty
        envVar = AloeEnv.EnvVar.ALOE_MAIL_PROVIDER;
        Assert.assertFalse(AloeEnv.inEnvVarList(envVar, "a"));
    }

    @Test
    public void testInEnvVarListPrefix()
    {
        AloeEnv.EnvVar envVar = AloeEnv.EnvVar.ALOE_REQUEST_LOGGING_FILTER_PREFIXES;
        String prefix = "/g";
        // Check that false is returned if either parameter is null
        Assert.assertFalse(AloeEnv.inEnvVarListPrefix(null,null));
        Assert.assertFalse(AloeEnv.inEnvVarListPrefix(envVar,null));
        Assert.assertFalse(AloeEnv.inEnvVarListPrefix(null,prefix));
        // Check that true is returned if value is a prefix in the list
        Assert.assertTrue(AloeEnv.inEnvVarListPrefix(envVar, prefix));
        // Check that false is returned if value is not in list
        Assert.assertFalse(AloeEnv.inEnvVarListPrefix(envVar, "/zzz"));
        // Check that false is returned if env var value is empty
        envVar = AloeEnv.EnvVar.ALOE_MAIL_PROVIDER;
        Assert.assertFalse(AloeEnv.inEnvVarListPrefix(envVar, "a"));
    }

    @Test
    public void testGetLogSecurityInfo()
    {
        boolean envVal = AloeEnv.getLogSecurityInfo();
        Assert.assertTrue(envVal);
    }
}