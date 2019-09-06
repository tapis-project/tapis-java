package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author ajamthe
 *
 */
public class MultifactorKBIRequestHandler implements UserInfo, UIKeyboardInteractive {
	/*
	 * ****************************************************************************
	 */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(MultifactorKBIRequestHandler.class);
	private final UIKeyboardInteractive interactive;	 
    private final UserInfo userInfo;
    private static final String[] KNOWN_MFA_PROMPTS = { 
	        "[sudo] password for: ", 
	        "TACC Token Code:",
	        "select one of the following options",
	        "Duo two-factor",
	        "Yubikey for "
	    };

    
    /**
     * @param userInfo
     * @param interactive
     */
    public MultifactorKBIRequestHandler( UserInfo userInfo, UIKeyboardInteractive interactive )
    {
        this.userInfo = userInfo;
        this.interactive = interactive;
    }
    
	    
	/**
	 *
	 */
	@Override
	public String[] promptKeyboardInteractive(String destination, String name, String instruction, String[] prompt, boolean[] echo) {
		
		{
			  if ( userInfo.getPassword() != null && isMFAPrompt(prompt[0]) && prompt.length != 0 )
			  {
			    prompt[0] = "Keyboard interactive required, supplied password is ignored\n" + prompt[0];
			  }
			  return interactive.promptKeyboardInteractive( destination, name, instruction, prompt, echo );
			}
	}
	 
	
	
    /**
     * Checks the given string for the presence of any of a known
     * set of MFA prompts given in {@link #KNOWN_MFA_PROMPTS}.
     * 
     * @param prompt the message returned from a kbi prompt
     * @return true if the prompt is a mfa challenge phrase, false otherwise
     */
    protected boolean isMFAPrompt(String prompt) {
        for(String knownMFAPrompt: KNOWN_MFA_PROMPTS) {
            if (StringUtils.contains(prompt, knownMFAPrompt)) {
               System.out.println("Found MFA prompt in the keyboard-interactive session");
                return true;
            }
        }
        return false;
    }

	

	@Override
	public String getPassphrase() {
		return userInfo.getPassphrase();
	}


	@Override
	public String getPassword() {
		return userInfo.getPassword();
	}


	@Override
	public boolean promptPassphrase(String message) {
		return userInfo.promptPassphrase(message);
	}


	@Override
	public boolean promptPassword(String message) {
		return userInfo.promptPassphrase(message);
	}


	@Override
	public boolean promptYesNo(String str) {
		return userInfo.promptYesNo(str);
	}


	@Override
	public void showMessage(String message) {
		  userInfo.showMessage(message);

	}
}



