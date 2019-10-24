package edu.utexas.tacc.tapis.sharedapi.jwt;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.RsaProvider;

/** This class tests a custom version of jjwt. That version of jjwt is modified in the 
 * following ways:
 * 
 *  1. jjwt customized for tapis is called jjwt-aloe and is available at
 *     https://github.com/richcar58/jjwt-aloe.
 *  2. The main changes are to pom.xml and SignatureAlgorithm.java.
 *  3. The built artifacts (jar files) are available from our local nexus server at
 *     https://maven.tacc.utexas.edu/nexus/content/repositories/thirdparty/io/jsonwebtoken/jjwt-aloe/0.9.1/.
 *
 * This program runs two tests.  The first (useKeyGen) uses jjwt facilities to generate
 * a key pair that is then used to sign and verify a JWT.  The second (useKeystore)
 * uses keys that pre-exist in a local keystore to sign and verify a JWT.  This latter
 * test simulates how Tapis will verify JWTs in develop, staging and production environments.     
 * 
 * NOTE: The pre-existing keys should be generated using the KeyManager program. 
 * 
 * This program source is JJWTTest.java. If any modification to the source is made, this program needs to be modified as well. 
 * 
 * @author rcardone
 */
@Test(groups= {"unit"})
public class JJWTTestNGTest 
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	private static final String TEST_KEY_STORE_PASSWORD = "!akxK3CuHfqzI#97";
	private static final String KEY_ALIAS = "jwt";

	// Predefined keystore file names.
	private static final String TEST_STORE_FILE_NAME  = ".TapisTestKeyStore.p12";

	/* ---------------------------------------------------------------------- */
	/* DataProvider: Setting Parameters for the test                          */
	/* ---------------------------------------------------------------------- */
	@DataProvider(name = "keyStoreFileCrendentials")
	public Object[][] createData1() {
		return new Object[][] {
			{TEST_STORE_FILE_NAME,KEY_ALIAS,TEST_KEY_STORE_PASSWORD} 
		};
	}

	/* ********************************************************************** */
	/*                              Tests                                     */
	/* ********************************************************************** */ 
	/* ----------------------------------------------------------------------- */
	/* useKeyGen:                                                              */
	/* ----------------------------------------------------------------------- */
	@Test(enabled=true)
	private void useKeyGen()
	{
		// Create keys.
		KeyPair keyPair = RsaProvider.generateKeyPair();

		// Assertions for public key
		Assert.assertEquals(keyPair.getPublic().getAlgorithm(),"RSA","Algorithm to generate public key ");
		Assert.assertEquals(keyPair.getPublic().getFormat(),"X.509","Public key format");

		//Assertions for private key
		Assert.assertEquals(keyPair.getPrivate().getAlgorithm(),"RSA","Algorithm to generate private key ");
		Assert.assertEquals(keyPair.getPrivate().getFormat(), "PKCS#8","Private key format");


		// Create and print json.
		// Note that setClaims needs called before setSubject otherwise subject information in the claim gets overwritten by setClaims
		Claims claims = Jwts.claims();
		claims.put("fruit", "banana");
		String json = Jwts.builder().setClaims(claims).setSubject("bud").signWith(keyPair.getPrivate(), SignatureAlgorithm.RS384).compact();

		// Validate JWT.
		try {
			Jwt jwt = Jwts.parser().setSigningKey(keyPair.getPublic()).parse(json);
			Header header = jwt.getHeader();
			Object body = jwt.getBody();
			Claims parsedClaims = (Claims) body;

			Assert.assertEquals(header.get("alg"),SignatureAlgorithm.RS384.toString(), "Signature algorithm did not match");
			Assert.assertEquals(parsedClaims.getSubject(),"bud", "Subject could not be verified");
			Assert.assertEquals(parsedClaims.get("fruit"),"banana","Claim could not be verified");

		} catch (SecurityException e) {
			System.out.println("Signature could not be verified!\n");
		}

	}

	/* ---------------------------------------------------------------------------- */
	/* useKeystore:                                                                 */
	/* ---------------------------------------------------------------------------- */
	/** Retrieve a key pair from the user-designated key store and create and verify
	 * a JWT using those keys.
	 * 
	 * @throws Exception on error
	 */
	@Test(dataProvider = "keyStoreFileCrendentials", enabled = false)
	private void useKeystore(String keystoreFilename, String alias, String password ) throws Exception
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

		// Create signed JWT
		// Note that setClaims needs called before setSubject otherwise subject information in the claim gets overwritten by setClaims
		Claims claims = Jwts.claims();
		claims.put("fruit", "apple");
		String json = Jwts.builder().setClaims(claims).setSubject("bud").signWith(keyPair.getPrivate(), SignatureAlgorithm.RS384).compact();

		try {
			Jwt jwt = Jwts.parser().setSigningKey(keyPair.getPublic()).parse(json);
			Header header = jwt.getHeader();
			Object body = jwt.getBody();
			Claims parsedClaims = (Claims) body;

			Assert.assertEquals(header.get("alg"),SignatureAlgorithm.RS384.toString(), "Signature algorithm did not match");
			Assert.assertEquals(parsedClaims.getSubject(),"bud", "Subject could not be verified");
			Assert.assertEquals(parsedClaims.get("fruit"),"apple","Claim could not be verified");

		} catch (SecurityException e) {
			System.out.println("Signature could not be verified!\n");
		}

	}
}

