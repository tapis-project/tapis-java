package edu.utexas.tacc.tapis.jobs.api.requestBody;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;

@Test(groups={"unit"})
public class ReqSubmitJobParseTest 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
	// The schema file for job submit requests.
    private static final String FILE_JOB_SUBMIT_REQUEST = 
            "/edu/utexas/tacc/tapis/jobs/api/jsonschema/SubmitJobRequest.json";

	/* ********************************************************************** */
	/*                                 Tests                                  */
	/* ********************************************************************** */
	@Test
	public void parseTest1() throws TapisException
	{
		// Test minimal input.
		String json = getInputTest1();
		parse(json);
	}
	
	@Test
	public void parseTest2() throws TapisException
	{
		// Test minimal parameters.
		String json = getInputTest2();
		parse(json);
	}
	
	@Test
	public void parseTest3() throws TapisException
	{
		// Test some parameters.
		String json = getInputTest3();
		parse(json);
	}
	
	@Test
	public void parseTest4() throws TapisException
	{
		// Test some parameters.
		String json = getInputTest4();
		parse(json);
	}
	
	@Test
	public void parseTest5() throws TapisException
	{
		// Test some parameters.
		String json = getInputTest5();
		parse(json);
	}
	
	@Test
	public void parseTest6() throws TapisException
	{
		// Test some parameters.
		String json = getInputTest6();
		parse(json);
	}
	
	/* ********************************************************************** */
	/*                            Private Methods                             */
	/* ********************************************************************** */
	private void parse(String json) throws TapisException
	{
        // Create validator specification.
        JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_JOB_SUBMIT_REQUEST);
        
        // Make sure the json conforms to the expected schema.
        try {JsonValidator.validate(spec);}
          catch (TapisJSONException e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            throw new TapisException(msg, e);
          }
	}
	
	private String getInputTest1()
	{
		String s = "{\"name\": \"bud\", \"appId\": \"app1\", \"appVersion\": \"v2.3\", "
				   + "\"description\": \"myJob\"}"; 
		return s;
	}

	private String getInputTest2()
	{
		String s = "{\"name\": \"harry\", \"appId\": \"app1\", \"appVersion\": \"v4.3\", "
				   + "\"description\": \"myJob\", "
				   + "\"parameters\": {}"
				   + "}"; 
		return s;
	}

	private String getInputTest3()
	{
		String s = "{\"name\": \"mary\", \"appId\": \"app1\", \"appVersion\": \"v6\", "
				   + "\"description\": \"myJob\", "
				   + "\"parameters\": {\"appParameters\": [\"x\", \"y\"], \"containerParameters\": []}"
				   + "}"; 
		return s;
	}

	private String getInputTest4()
	{
		String s = "{\"name\": \"mary\", \"appId\": \"app1\", \"appVersion\": \"v6\", "
				   + "\"description\": \"myJob\", "
				   + "\"parameters\": {\"execSystemConstraints\": [], \"envVariables\": []}"
				   + "}"; 
		return s;
	}

	private String getInputTest5()
	{
		String s = "{\"name\": \"mary\", \"appId\": \"app1\", \"appVersion\": \"v6\", "
				   + "\"description\": \"myJob\", "
				   + "\"parameters\": {\"execSystemConstraints\": ["
				   + "{\"key\": \"key1\", \"value\": \"stringVal\"}, "
				   + "{\"key\": \"key2\", \"value\": 3.8}, "
				   + "{\"key\": \"key3\", \"value\": 7}, "
				   + "{\"key\": \"key4\", \"value\": true}, "
				   + "{\"key\": \"key5\", \"value\": null}]"
				   + "}}"; 
		return s;
	}

	private String getInputTest6()
	{
		String s = "{\"name\": \"mary\", \"appId\": \"app1\", \"appVersion\": \"v7\", "
				   + "\"description\": \"myJob\", "
				   + "\"inputs\": ["
				   + "{\"sourceUrl\": \"tapis://host.com/path\", \"targetPath\": \"newFileName\"}, "
				   + "{\"sourceUrl\": \"sftp://host.com/path\", \"targetPath\": \"\"}, "
				   + "{\"sourceUrl\": \"https://host.com/path\"} "
				   + "]}"; 
		return s;
	}
}
