package edu.utexas.tacc.tapis.sharedapi.keys;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Thin wrapper around KeyStore that provides convenient methods for key pair
 * retrieval.  This class is used during JWT signing and signature validation. 
 * 
 * @author rcardone
 */
public class KeyManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(KeyManager.class);
    
    // Default store location.
    public static final String STORE_FILE_NAME  = ".TapisKeyStore.p12";
    
    // Type of keystore used by this manager.
    private static final String STORE_TYPE = "PKCS12";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The real key store.
    private KeyStore _keyStore;
    
    // Store location.
    private String _storePath;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create manage using default key store path.
     * 
     * @throws KeyStoreException
     */
    public KeyManager() throws KeyStoreException
    {
        // Use the default location.
        this(null, null);
    }
    
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Initialize a key store instance.  If either of the parameters are null
     * default values will be used.
     * 
     * @param baseDir directory in which key store file resides or null
     * @param storeFilename name of key store file or null
     * @throws KeyStoreException failure to access a key store instance
     */
    public KeyManager(String baseDir, String storeFilename) throws KeyStoreException
    {
        // Check input.
        if (StringUtils.isBlank(baseDir)) baseDir = getHomeDir();
        if (!baseDir.endsWith(File.separator)) baseDir += File.separator;
        if (StringUtils.isBlank(storeFilename)) storeFilename = STORE_FILE_NAME;
        
        // Assign the store path.
        _storePath = baseDir + storeFilename;
        
        // Get the key store instance.
        try {_keyStore = KeyStore.getInstance(STORE_TYPE);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_INSTANCE_FAILURE", STORE_TYPE);
                _log.error(msg, e);
                throw e;
            }
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* load:                                                                  */
    /* ---------------------------------------------------------------------- */
    /** Load the key store given by the _storePath field.  If the file doesn't
     * exist, the file and the path to it will be created.
     * 
     * @param password
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws IOException
     * @throws KeyStoreException
     */
    public void load(String password) 
     throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException
    {
        // See if the store already exists.
        File ksFile = new File(_storePath);
        if (!ksFile.exists()) {
            // Initialize the keystore to make the keystore available.
            _keyStore.load(null, null);
            
            // Create the parent directory if necessary.
            File ksDir = ksFile.getParentFile();
            if (!ksDir.exists()) {
                // Make the directory chain.
                ksDir.mkdirs();
            
                // Restrict access to this directory to the creator only.
                HashSet<PosixFilePermission> perms = new HashSet<>();
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_WRITE);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                java.nio.file.Files.setPosixFilePermissions(ksDir.toPath(), perms);
            }
            
            // Create the store file--this only takes a while when debugging.
            _keyStore.store(new FileOutputStream(ksFile.getAbsolutePath()), 
                            password.toCharArray());
            
            // Restrict access to the key store file.
            HashSet<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            java.nio.file.Files.setPosixFilePermissions(ksFile.toPath(), perms);
        }
        else {
            // Load the store file--this only takes a while when debugging.
            _keyStore.load(new FileInputStream(ksFile.getAbsolutePath()), 
                           password.toCharArray());
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getPrivateKey:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Retrieve the named private key from the key store.
     * 
     * @param alias the name given to the key
     * @param password the key's password, which might be the store password
     * @return the private key
     * @throws UnrecoverableKeyException
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     */
    public PrivateKey getPrivateKey(String alias, String password) 
     throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException
    {
       return (PrivateKey) _keyStore.getKey(alias, password.toCharArray()); 
    }

    /* ---------------------------------------------------------------------- */
    /* getCertificate:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Get the signed certificate containing the public key associated with the
     * alias.
     * 
     * @param alias
     * @return the certificate 
     * @throws KeyStoreException
     */
    public Certificate getCertificate(String alias) 
     throws KeyStoreException
    {
       return _keyStore.getCertificate(alias); 
    }
    
    /* ---------------------------------------------------------------------- */
    /* getStorePath:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Key store path accessor.
     * @return the path to the keystore
     */
    public String getStorePath() {return _storePath;}

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getHomeDir:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Always return a non-empty string, falling back on the root directory if 
     * necessary.
     * @return the home directory string
     */
    private static String getHomeDir()
    {
        // Query the environment for the home directory.
        String home = System.getenv("HOME");
        if (StringUtils.isBlank(home)) return File.separator;
          else return home;
    }
}
