package edu.utexas.tacc.tapis.shared.parameters;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.AloeConstants;
import edu.utexas.tacc.tapis.shared.exceptions.AloeException;
import edu.utexas.tacc.tapis.shared.parameters.AloeEnv;
import edu.utexas.tacc.tapis.shared.parameters.AloeInput;


@Test(groups={"unit"})
public class AloeInputTest
{
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AloeInputTest.class);
    // Save copy of existing env for restoring later
    private Map<String, String> oldEnv = new HashMap<>(System.getenv());

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
     * Check that properties are correctly set from env variables for:
     *    env var of form aloe.envonly.allow.test.query.parms
     *    env var of form ALOE_ENVONLY_JWT_OPTIONAL
     *    env var of form aloe.envonly... has precedence over ALOE_ENVONLY...
     * ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void testGetInputParameters() throws AloeException
    {
        // Process the env variables
        AloeInput aloeInput = new AloeInput(AloeConstants.SERVICE_NAME_JOBS);
        Properties aloeProperties = aloeInput.getInputParameters();

        // Check EnvVar.getEnvName
        String propName = AloeEnv.EnvVar.ALOE_ENVONLY_ALLOW_TEST_QUERY_PARMS.getEnvName();
        String checkName = propName;
        String propVal = aloeProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was empty for: " + checkName);
        Assert.assertEquals(propVal, "true");

        // Check EnvVar.name
        propName = AloeEnv.EnvVar.ALOE_ENVONLY_JWT_OPTIONAL.getEnvName();
        checkName = AloeEnv.EnvVar.ALOE_ENVONLY_JWT_OPTIONAL.name();
        propVal = aloeProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was empty for: " + checkName);
        Assert.assertEquals(propVal, "true");

        // Check that EnvVar.getEnvName has precedence over EnvVar.name
        propName = AloeEnv.EnvVar.ALOE_ENVONLY_KEYSTORE_PASSWORD.getEnvName();
        checkName = propName;
        propVal = aloeProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was null for: " + checkName);
        Assert.assertEquals(propVal, "value1");
    }
}