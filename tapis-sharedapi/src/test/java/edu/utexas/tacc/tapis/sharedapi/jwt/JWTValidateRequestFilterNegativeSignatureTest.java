package edu.utexas.tacc.tapis.sharedapi.jwt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWT;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWTParameters;

/** 
 *  This class performs a negative signature test on JWTValidateRequestFilter.java
 *  We create a JWT with correct claim and sign it with an incorrect key, i.e. with a Test key.
 *  The test will pass if the JWTValidateRequestFilter can detect that the signature could not be verified.
 * 
 * @author spadhy
 */

@Test(groups = { "integration" })
public class JWTValidateRequestFilterNegativeSignatureTest {

	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	// Predefined Inputfile
	private static final String TEST_INPUTFILE_NAME = "src/test/resources/testuser3Claims.json";

	// Predefined test keystore file name.
	private static final String TEST_STORE_FILE_NAME = ".TapisTestKeyStore.p12";

	// Predefined test keystore password
	private static final String TEST_KEY_STORE_PASSWORD = "!akxK3CuHfqzI#97";

	private static final String PATH_TO_JOB_FILE = "src/test/resources/helloworldjob-tapis.json";

	private static final String TAPIS_HOST = "http://localhost:8080/jobs/v2?pretty=true";
	
	private static final String KEY_ALIAS = "jwt";

	/* **************************************************************************** */
	/*                                   Fields	                                    */
	/* **************************************************************************** */

	// Path to an output file where the generated JWTs are written
	private String pathToJWTOutputFileWithIncorrectSignature = "";

	// Temporary directory path
	private Path newTapisTempDir = null;

	// Temporary file paths
	private Path tmpFileWithIncorrectSignature = null;
	private Path tmpErrorFileForIncorrectSignature = null;

	private String jwtWithIncorrectSignature = "";

	/* ********************************************************************** */
	/*                            Set Up                                      */
	/* ********************************************************************** */  
	/* ---------------------------------------------------------------------- */
	/* setup:                                                                 */
	/* ---------------------------------------------------------------------- */
	@BeforeSuite
	public void setup() throws Exception {

		// Create a JWT for TestUser 3 with correct claim using Test key instead of Production key
		
		// Get the default temporary directory
		String tmpdir = System.getProperty("java.io.tmpdir");

		// Create a temporary directory inside the default temporary directory
		newTapisTempDir = Files.createTempDirectory(Paths.get(tmpdir), "tapis-jwt");

		// Create a temporary file for JWT to be written with a prefix and a suffix inside the newly created temporary directory
		tmpFileWithIncorrectSignature = Files.createTempFile(newTapisTempDir, "testuser3Claims", ".jwt");

		pathToJWTOutputFileWithIncorrectSignature = tmpFileWithIncorrectSignature.toAbsolutePath().toString();
		System.out.println("Path to JwtOutputFile: " + pathToJWTOutputFileWithIncorrectSignature + "\n");

		// Create a temporary error file
		tmpErrorFileForIncorrectSignature = Files.createTempFile(newTapisTempDir, "errorIncorrectSignature", ".txt");

		// Create a JWT with correct claim but signing it with Test key
		String[] args = { "-i", TEST_INPUTFILE_NAME, "-k", TEST_STORE_FILE_NAME, "-p", TEST_KEY_STORE_PASSWORD, 
		        "-a", KEY_ALIAS, "-o", pathToJWTOutputFileWithIncorrectSignature };
		CreateJWTParameters parms;

		parms = new CreateJWTParameters(args);
		CreateJWT cj = new CreateJWT(parms);
		cj.exec();

		BufferedReader input = new BufferedReader(new FileReader(pathToJWTOutputFileWithIncorrectSignature));
		jwtWithIncorrectSignature = input.readLine(); //jwt is a base64url encoded string with no newline

		input.close();
	}
	
	/* ********************************************************************** */
	/*                              Test                                      */
	/* ********************************************************************** */  
	@Test(enabled = true)
	public void negativeSignatureTest() throws IOException, InterruptedException {

		ArrayList<String> cmdList = new ArrayList<>(12);
        
		//create curl command
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
		cmdList.add("x-jwt-assertion-iplantc-org:" + jwtWithIncorrectSignature);
		cmdList.add(TAPIS_HOST);

		// Create the process builder.
		ProcessBuilder pb = new ProcessBuilder(cmdList);
		pb.redirectOutput(tmpErrorFileForIncorrectSignature.toFile());
		pb.redirectError(tmpErrorFileForIncorrectSignature.toFile());

		Process process = pb.start();
		int rc = process.waitFor();

		String line = "";

		if (rc != 0) {
			Assert.assertEquals(rc, 0, "negativeSignatureTest: Sub-process terminated with exit value 1");

		} else {
			BufferedReader outputFile = new BufferedReader(new FileReader(tmpErrorFileForIncorrectSignature.toFile()));
			ArrayList<String> lines = new ArrayList<>();
			while ((line = outputFile.readLine()) != null) {
				lines.add(line);
			}
			String errorMsg = "TAPIS_SECURITY_JWT_PARSE_ERROR Unable to parse JWT: TAPIS_SECURITY_JWT_PARSE_ERROR Unable to parse JWT: JWT signature does not match locally computed signature. JWT validity cannot be asserted and should not be trusted.";
			Assert.assertTrue(lines.get(0).equals(errorMsg) || lines.get(2).equals("Code: 401"),
					"Signature verification test failed");
			outputFile.close();
		}
	}
	
	/* ********************************************************************** */
	/*                           CleanUp                                      */
	/* ********************************************************************** */ 
	@AfterSuite
	public void cleanup() throws IOException {
		Files.delete(tmpFileWithIncorrectSignature.toAbsolutePath());
		Files.delete(tmpErrorFileForIncorrectSignature.toAbsolutePath());

		Files.delete(newTapisTempDir);
	}
}
