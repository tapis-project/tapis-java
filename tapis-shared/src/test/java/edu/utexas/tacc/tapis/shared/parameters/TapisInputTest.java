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

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisInput;


@Test(groups={"unit"}, enabled = false)
public class TapisInputTest
{
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(TapisInputTest.class);
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
        //   tapis.envonly.allow.test.query.parms=true
        //   TAPIS_ENVONLY_JWT_OPTIONAL=true
        //   tapis.envonly.keystore.password=value1
        //   TAPIS_ENVONLY_KEYSTORE_PASSWORD=value2
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS.getEnvName(), "true");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL.name(), "true");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD.getEnvName(), "value1");
        newEnv.put(TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD.name(), "value2");
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
     * Check that properties are correctly set from env variables for:
     *    env var of form tapis.envonly.allow.test.query.parms
     *    env var of form TAPIS_ENVONLY_JWT_OPTIONAL
     *    env var of form tapis.envonly... has precedence over TAPIS_ENVONLY...
     * ---------------------------------------------------------------------------- */
    @Test(enabled=false)
    public void testGetInputParameters() throws TapisException
    {
        // Process the env variables
        TapisInput tapisInput = new TapisInput(TapisConstants.SERVICE_NAME_JOBS);
        Properties tapisProperties = tapisInput.getInputParameters();

        // Check EnvVar.getEnvName
        String propName = TapisEnv.EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS.getEnvName();
        String checkName = propName;
        String propVal = tapisProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was empty for: " + checkName);
        Assert.assertEquals(propVal, "true");

        // Check EnvVar.name
        propName = TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL.getEnvName();
        checkName = TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL.name();
        propVal = tapisProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was empty for: " + checkName);
        Assert.assertEquals(propVal, "true");

        // Check that EnvVar.getEnvName has precedence over EnvVar.name
        propName = TapisEnv.EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD.getEnvName();
        checkName = propName;
        propVal = tapisProperties.getProperty(propName);
        Assert.assertNotNull(propVal, "Property from environment was null for: " + checkName);
        Assert.assertFalse(propVal.isEmpty(), "Property from environment was null for: " + checkName);
        Assert.assertEquals(propVal, "value1");
    }
}