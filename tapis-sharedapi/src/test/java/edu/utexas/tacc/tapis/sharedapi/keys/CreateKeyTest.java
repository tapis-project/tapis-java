package edu.utexas.tacc.tapis.sharedapi.keys;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.sharedapi.utils.CreateKey;
import edu.utexas.tacc.tapis.sharedapi.utils.CreateKeyParameters;

/**
 * This class tests the CreateKey class
 * @author spadhy
 *
 */
@Test(groups= {"unit"})
public class CreateKeyTest {

	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	private static final String TEST_STORE_FILE_NAME = ".TapisCreateKeyTest.p12";
	private static final String TEST_KEY_STORE_PASSWORD = "aloe!23tyu#xsre98";
	private static final String TEST_ALIAS = "testkey";
	private static final String STORE_TYPE = "PKCS12";

	/* ********************************************************************** */
	/* 								Fields                                    */
	/* ********************************************************************** */

	private File keystoreFile;
	private KeyStore testKeystore;
	private String homeDir;

	/* ********************************************************************** */
	/*                            Set Up                                      */
	/* ********************************************************************** */    
	/* ---------------------------------------------------------------------- */
	/* setup:                                                                 */
	/* ---------------------------------------------------------------------- */
	@BeforeSuite
	public void setup() throws IOException {
		homeDir = System.getenv("HOME");
	}

	/* ********************************************************************** */
	/*                              Test                                      */
	/* ********************************************************************** */  
	@Test(enabled = true)
	public void createKeyTest() throws Exception {
		String[] args = { "-a", TEST_ALIAS, "-k", TEST_STORE_FILE_NAME, "-p", TEST_KEY_STORE_PASSWORD, "-u",
		                  "aloe agave" };
		CreateKeyParameters parms = new CreateKeyParameters(args);
		CreateKey ck = new CreateKey(parms);
		ck.exec();

		// Check if the keystore file is created in the home directory
		keystoreFile = new File(homeDir + File.separator + TEST_STORE_FILE_NAME);
		Assert.assertTrue(keystoreFile.exists(), "Keystorefile does not exist");

		// Load the newly created keystore file
		// Check if the file has an entry for the TEST_ALIAS
		testKeystore = KeyStore.getInstance(STORE_TYPE);
		FileInputStream fis = null;
		char[] testKeystorePassword = TEST_KEY_STORE_PASSWORD.toCharArray();
		fis = new FileInputStream(homeDir + File.separator + TEST_STORE_FILE_NAME);
		testKeystore.load(fis, testKeystorePassword);
		Assert.assertTrue(testKeystore.isKeyEntry(TEST_ALIAS),"Keystore does not have an entry to the corresponding alias");

		// Get the Private Key
		PrivateKey privateKey = (PrivateKey) testKeystore.getKey(TEST_ALIAS, testKeystorePassword);

		// Get the certificate
		Certificate cert = testKeystore.getCertificate(TEST_ALIAS);

		// Get the public key
		PublicKey publicKey = cert.getPublicKey();

		// verify the certificate
		cert.verify(publicKey);

		// Preparing to sign the data
		KeyPair keyPair = new KeyPair(publicKey, privateKey);

		// The bytes array for the data
		byte[] data = "test".getBytes("UTF8");

		// Get an instance of the Signature object with Signing algorithm
		Signature sigToSign = Signature.getInstance("SHA1WithRSA");

		// Initialize with the private key
		sigToSign.initSign(keyPair.getPrivate());

		// Provide data for signing
		sigToSign.update(data);

		// Generate signature
		byte[] signatureBytes = sigToSign.sign();

		// Preparing for verifying the signature
		// Get an instance of the signature object with same signing algorithm
		Signature sigToVerify = Signature.getInstance("SHA1WithRSA");

		// Initialize with public key
		sigToVerify.initVerify(keyPair.getPublic());

		// Provide the data for which verification is being done
		sigToVerify.update(data);

		Assert.assertTrue(sigToVerify.verify(signatureBytes), "Signature could not verified");
	}

	/* ********************************************************************** */
	/*                           CleanUp                                      */
	/* ********************************************************************** */
	// Delete the  keystore file created by createKey
	@AfterSuite
	public void cleanup() {
		try {
			Files.delete(Paths.get(homeDir, TEST_STORE_FILE_NAME));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
