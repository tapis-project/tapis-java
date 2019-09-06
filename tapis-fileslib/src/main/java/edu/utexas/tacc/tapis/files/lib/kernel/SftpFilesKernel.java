package edu.utexas.tacc.tapis.files.lib.kernel;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


/**
 * @author ajamthe
 *
 */
public class SftpFilesKernel {
	/*
	 * ****************************************************************************
	 */
	/* Constants */
	/*
	 * ****************************************************************************
	 */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(SftpFilesKernel.class);
	
	// Socket timeouts
    private static final int CONNECT_TIMEOUT_MILLIS = 15000;   // 15 seconds
    private static final String STRICT_HOSTKEY_CHECKIN_KEY = "StrictHostKeyChecking";
	private static final String STRICT_HOSTKEY_CHECKIN_VALUE = "no";
	private static final String CHANNEL_TYPE = "sftp";


    
	/*
	 * ****************************************************************************
	 */
	/* Fields */
	/*
	 * ****************************************************************************
	 */
	private String host;
	private int port;
	private String username;
	private String password;
	private String publicKey;
	private String privateKey;
	private Session session;
	private Channel channel;
	private ChannelSftp sftpChannel;
	private String authMethod;
	private boolean authPass = false;
	
	/* ********************************************************************** */
	/* Constructors */
	/* ********************************************************************** */
	
	/**
	 * @param host // Destination host name for files/dir copy
	 * @param user //user having access to the remote system
	 * @param port //connection port number
	 * @param publicKey //public Key of the user 
	 * @param privateKey //private key of the user 
	 * This constructor will get called if user chooses ssh keys to authenticate to remote host
	 * This will set a authKey flag to true
	 */
	public SftpFilesKernel(String host, String username, int port, String publicKey, String privateKey) {
		super();
		this.host = host;
		this.username = username;
		this.port = port > 0 ? port : 22;
		this.privateKey = privateKey;
		authMethod = "publickeyAuth";
	}
		
	
	/**
	 * @param host // Destination host name for files/dir copy
	 * @param port // connection port number
	 * @param username //user having access to the remote system
	 * @param password //password
	 * This constructor will get called if user chooses password to authenticate to remote host
	 * This will set the authPass flag to true
	 */
	public SftpFilesKernel(String host, int port, String username, String password) {
		super();
		this.host = host; 
		this.port = port > 0 ? port : 22;
		this.username = username;
		this.password = password;
		authMethod = "passwordAuth";
	}

	
   /**
    * @throws JSchException
    * @throws IOException
    */
	public void openSession() throws JSchException, IOException {
	   
		//Create a new JSch object
		final JSch jsch = new JSch();
	 
		
		//Instantiates the Session object with username and host. 
		//The TCP port 22 will be used in making the connection. 
		//Note that the TCP connection must not be established until Session#connect().
		//return instance of session class
		session = jsch.getSession(username, host, port);
	
		session.setConfig(STRICT_HOSTKEY_CHECKIN_KEY, STRICT_HOSTKEY_CHECKIN_VALUE);
		 session.setConfig("PreferredAuthentications",
                 "password,gssapi-with-mic,publickey,keyboard-interactive");
      
		session.setTimeout(CONNECT_TIMEOUT_MILLIS);

		//if(session.getConfig(PreferredAuthentications).contains("publickey")) {
		if(authMethod.equalsIgnoreCase("publicKeyAuth")) {
		//Adds an identity to be used for public-key authentication
			jsch.addIdentity(privateKey);
			_log.info("identity for public-key authentication successfully added");
		}
		//else if(session.getConfig(PreferredAuthentications).contains("password")) {
		if(authMethod.equalsIgnoreCase("passwordAuth")){
			//Adds password to be used for password based authentication
    		session.setPassword(password);
		}
			
		// Get a connection
		_log.debug("Try to connect to the host " + host +":" + port +" with user "+ username);
		session.connect();
		
		_log.debug("Connection established");
        
		//Open SSH Channel
		_log.debug("Open SSH Channel");
		channel = session.openChannel("sftp");
		channel.connect();
		_log.debug("Channel open OK");
   }
	
	
   /**
    * @throws Exception
    */
   public void close() throws Exception {
		// Close channel
		channel.disconnect();
		// Close session
		session.disconnect();
	}


	public void put(String source, String destination) {
		try {
			ChannelSftp channelsftp = (ChannelSftp) channel;
			ProgressMonitor progress = new ProgressMonitor();
			channelsftp.put(source, destination, progress);

		} catch (Exception e) {
			_log.debug(e.getMessage());
		}
	}

	

	
}
