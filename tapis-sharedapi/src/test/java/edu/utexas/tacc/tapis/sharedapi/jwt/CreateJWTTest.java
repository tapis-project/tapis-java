package edu.utexas.tacc.tapis.sharedapi.jwt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWT;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateJWTParameters;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;


/**
 * This class tests the CreateJWT class
 * @author spadhy
 *
 */
@Test(groups= {"unit"})
public class CreateJWTTest 
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	// Predefined Inputfile
	private static final String TEST_INPUTFILE_NAME = "src/test/resources/testuser2Claims.json";

	// Predefined keystore file names.
	private static final String TEST_STORE_FILE_NAME  = ".TapisTestKeyStore.p12";

	// Predefined test keystore password
	private static final String TEST_KEY_STORE_PASSWORD = "!akxK3CuHfqzI#97";

	private static final String KEY_ALIAS = "jwt";

	/* ********************************************************************** */
	/*                                 Fields                                 */
	/* ********************************************************************** */

	// Path to an output file where the generated JWT is written
	private String pathToJWTOutputFile = "";

	// Temporary directory path
	private Path newTapisTempDir = null;

	//Temporary file path
	private Path tmpFile = null;

	/* ********************************************************************** */
	/*                            Set Up                                      */
	/* ********************************************************************** */    
	/* ---------------------------------------------------------------------- */
	/* setup:                                                                 */
	/* ---------------------------------------------------------------------- */
	@BeforeSuite
	public void setup() throws IOException {

		//Get the default temporary directory
		String tmpdir = System.getProperty("java.io.tmpdir");

		//Create a temporary directory inside the default temporary directory
		newTapisTempDir = Files.createTempDirectory(Paths.get(tmpdir), "tapis-jwt");

		//Create a temporary file with a prefix and a suffix inside the newly created temporary directory 
		tmpFile = Files.createTempFile(newTapisTempDir,"testuser2Claims", ".jwt");

		pathToJWTOutputFile = tmpFile.toAbsolutePath().toString() ;
		System.out.println("Path to JwtOutputFile: "+ pathToJWTOutputFile +"\n");
	}

	/* ********************************************************************** */
	/*                              Test                                      */
	/* ********************************************************************** */  

	@Test(enabled=false)
	public void createJWTTest() throws Exception
	{

		String[] args= {"-i",TEST_INPUTFILE_NAME, "-k", TEST_STORE_FILE_NAME,"-p",TEST_KEY_STORE_PASSWORD, "-a", KEY_ALIAS, "-o",pathToJWTOutputFile};	// note that aliases in CreatJWT does not work
		CreateJWTParameters parms;

		parms = new CreateJWTParameters(args);
		CreateJWT cj = new CreateJWT(parms);
		cj.exec();

		// validating JWT
		validateJWT(pathToJWTOutputFile, TEST_STORE_FILE_NAME, KEY_ALIAS, TEST_KEY_STORE_PASSWORD);

	}

	/* ********************************************************************** */
	/*                           CleanUp                                      */
	/* ********************************************************************** */ 
	@AfterSuite
	public void cleanup() throws IOException {

		Files.delete(tmpFile.toAbsolutePath());
		Files.delete(newTapisTempDir);
	}

	/* **************************************************************************** */
	/*                               Private Method                                 */
	/* **************************************************************************** */
	private void validateJWT(String jwtFile,String keystoreFilename, String alias, String password ) throws Exception
	{
		// ----- Load the keystore.
		KeyManager km = new KeyManager(null, keystoreFilename);
		km.load(password);

		// ----- Get the private key from the keystore.
		System.out.println("Retrieving private key and certificate containing public key.");
		PrivateKey privateKey = km.getPrivateKey(alias, password);
		Certificate cert = km.getCertificate(alias);
		PublicKey publicKey = cert.getPublicKey();

		// ----- Verify certificate.
		System.out.println("Verifying the certificate.");
		cert.verify(publicKey);
		KeyPair keyPair = new KeyPair(publicKey, privateKey);


		// Validate JWT.
		System.out.println("----------Validating JWT----------");

		BufferedReader input = new BufferedReader(new FileReader(jwtFile));
		String json = input.readLine();
		try {
			Jwt jwt = Jwts.parser().setSigningKey(keyPair.getPublic()).parse(json);
			Header header = jwt.getHeader();
			Object body = jwt.getBody();
			Claims parsedClaims = (Claims) body;

			Assert.assertEquals(header.get("alg"),SignatureAlgorithm.RS256.toString(), "Signature algorithm did not match");
			Assert.assertEquals(parsedClaims.get("http://wso2.org/claims/subscriber"),"testuser2", "Subscriber could not be verified");
			Assert.assertEquals(parsedClaims.get("iss"),"wso2.org/products/am","Claim could not be verified");

		} catch (SignatureException e) {
			System.out.println("Signature could not be verified!\n");
		}

	}


}
