package edu.utexas.tacc.tapis.sharedapi.jwt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWT;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWTParameters;

/** 
 *  This class performs a negative claim test on JWTValidateRequestFilter.java
 *  We create a JWT with correct claim and sign it with correct production key.
 *  A JWT is of the format base64urlencoded(header).base64urlencoded(payload).signature
 *  In this test, we replace the base64urlencoded(payload) in the correct JWT with 
 *  a base64urlencoded(incorrect claim). The test will pass if the JWTValidateRequestFilter 
 *  can detect that the payload has been tampered,i.e.the integrity of the message is not preserved.
 * 
 * @author spadhy
 */

@Test(groups= {"integration"})
public class JWTValidateRequestFilterNegativeClaimTest {

	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */

	private static final String PATH_TO_JOB_FILE = "src/test/resources/helloworldjob-tapis.json";

	private static final String TAPIS_HOST = "http://localhost:8080/jobs/v2?pretty=true";

	private static final String CORRECT_CLAIM_INPUTFILE_NAME  =  "src/test/resources/testuser3Claims.json";

	private static final String KEY_ALIAS = "jwt";
	   
	/* **************************************************************************** */
	/*                                   Fields	                                    */
	/* **************************************************************************** */

	// Path to an output file where the generated JWT is written

	private String pathToJWTOutputFileWithCorrectClaim = "";

	// Temporary directory path
	private Path newTapisTempDir = null;

	// Temporary file paths

	private Path tmpFileWithCorrectClaim = null;

	private Path tmpErrorFileForIncorrectClaim = null;

    // JWTs
	private String jwtWithIncorrectClaim = "";

	private String jwtWithCorrectClaim = "";


	private String passwordProdKeyStore = TapisEnv.get(EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD);

	/* ********************************************************************** */
	/*                            Set Up                                      */
	/* ********************************************************************** */    
	/* ---------------------------------------------------------------------- */
	/* setup:                                                                 */
	/* ---------------------------------------------------------------------- */
	@BeforeSuite
	public void setup() throws Exception {

		// Create a JWT for TestUser 3 with Correct Claim using Production Key

		// Get the default temporary directory
		String tmpdir = System.getProperty("java.io.tmpdir");

		// Create a temporary directory inside the default temporary directory
		newTapisTempDir = Files.createTempDirectory(Paths.get(tmpdir), "tapis-jwt");

		/* ************************************************************************ */
		/* Temporary files for negative claim test                                  */
		/* ************************************************************************ */

		tmpErrorFileForIncorrectClaim = Files.createTempFile(newTapisTempDir, "errorInCorrectClaim", ".txt");

		tmpFileWithCorrectClaim = Files.createTempFile(newTapisTempDir, "testuser3CorrectClaims", ".jwt");

		pathToJWTOutputFileWithCorrectClaim = tmpFileWithCorrectClaim.toAbsolutePath().toString();

		/* ************************************************************************ */
		/* Create JWT for negative claim test                                       */
		/* ************************************************************************ */
		// Right Signature but Incorrect claim

		String[] args = { "-i", CORRECT_CLAIM_INPUTFILE_NAME, "-p", passwordProdKeyStore, "-a", KEY_ALIAS, 
		        "-o", pathToJWTOutputFileWithCorrectClaim };

		CreateJWTParameters parms;

		parms = new CreateJWTParameters(args);
		CreateJWT cj = new CreateJWT(parms);
		cj.exec();

		BufferedReader input = new BufferedReader(new FileReader(pathToJWTOutputFileWithCorrectClaim));
		jwtWithCorrectClaim = input.readLine(); // jwt is a base64url encoded string with no newline
		System.out.println("JWT with Correct claim:   " + jwtWithCorrectClaim + " \n");
		input.close();

		/* ************************************************************************ */
		/* Create Incorrect claim                                                   */
		/* ************************************************************************ */
		// Create a claim in JSON format
		JsonObject json = new JsonObject();
		json.addProperty("name", "random user");
		json.addProperty("bank", "fcu");

		// Base64URL encode the incorrect claim
		Encoder b64Encoder = Base64.getUrlEncoder();
		String encodedPayload = b64Encoder.encodeToString(json.toString().getBytes());
		Decoder b64Decoder = Base64.getUrlDecoder();
		String decodedPayload = b64Decoder.decode(encodedPayload).toString();

		System.out.println("encoded Incorrect Payload: " + encodedPayload + "\n");
		System.out.println("decoded Incorrect Payload: " + decodedPayload + "\n");

		/* ************************************************************************ */
		/* Create JWT with Incorrect claim and correct signature                    */
		/* ************************************************************************ */
		String[] jwtArrayCorrect = jwtWithCorrectClaim.split("\\.");

		jwtWithIncorrectClaim = jwtArrayCorrect[0] + "." + encodedPayload + "." + jwtArrayCorrect[2];
		System.out.println("JWT with Incorrect claim:   " + jwtWithIncorrectClaim + " \n");
	}

	/* ********************************************************************** */
	/*                              Test                                      */
	/* ********************************************************************** */  
	@Test(enabled = true)
	public void negativeClaimTest() throws IOException, InterruptedException {

		ArrayList<String> cmdList = new ArrayList<>(12);

		// create curl command
		cmdList.add("curl");
		cmdList.add("-w");
		cmdList.add("\nType: %{content_type}\nCode: %{response_code}\n");
		cmdList.add("-X");
		cmdList.add("POST");
		cmdList.add("--data");
		cmdList.add("@" + PATH_TO_JOB_FILE);
		cmdList.add("-H");
		cmdList.add("Content-Type: application/json");
		cmdList.add("-H");
		cmdList.add("x-jwt-assertion-iplantc-org:" + jwtWithIncorrectClaim);
		cmdList.add(TAPIS_HOST);

		// Create the process builder.
		ProcessBuilder pb = new ProcessBuilder(cmdList);
		pb.redirectOutput(tmpErrorFileForIncorrectClaim.toFile());
		pb.redirectError(tmpErrorFileForIncorrectClaim.toFile());

		Process process = pb.start();
		int rc = process.waitFor();

		String line = "";

		if (rc != 0) {
			Assert.assertEquals(rc, 0, "negativeClaimTest:Sub-process terminated with exit value 1");

		} else {

			BufferedReader outputFile = new BufferedReader(new FileReader(tmpErrorFileForIncorrectClaim.toFile()));
			ArrayList<String> lines = new ArrayList<>();
			while ((line = outputFile.readLine()) != null) {
				lines.add(line);
			}
			String errorMsg = "TAPIS_SECURITY_JWT_PARSE_ERROR";
			Assert.assertTrue(lines.get(0).contains(errorMsg) && lines.get(2).equals("Code: 401"),
					"Claim verification test failed");
			outputFile.close();
		}
	}
	
	/* ********************************************************************** */
	/*                           CleanUp                                      */
	/* ********************************************************************** */ 
	@AfterSuite
	public void cleanup() throws IOException {

		Files.delete(tmpFileWithCorrectClaim.toAbsolutePath());
		Files.delete(tmpErrorFileForIncorrectClaim.toAbsolutePath());

		Files.delete(newTapisTempDir);
	}
}
