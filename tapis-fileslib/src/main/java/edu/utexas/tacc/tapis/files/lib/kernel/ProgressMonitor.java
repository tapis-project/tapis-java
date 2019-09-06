package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.SftpProgressMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProgressMonitor implements SftpProgressMonitor {
	/*
	 * ****************************************************************************
	 */
	/* Constants */
	/*
	 * ****************************************************************************
	 */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(ProgressMonitor.class);
	
	private long max = 0;
	private long count = 0;
	private long percent = 0;
	//private CallbackContext callbacks = null;
	
    public ProgressMonitor() {}
 
	public boolean count(long bytes) {
		this.count += bytes;
		long percentNow = this.count*100/max;
		if ( percentNow > this.percent) {
				this.percent = percentNow;
				_log.debug("progress " + this.percent); // Progress 0,0
				_log.debug("Total file size: "+ max + " bytes"); //total file size
				_log.debug("Bytes copied: "+ this.count);//Progress in bytes from the total
		}
		return true;
	}

	public void end() {
		_log.debug("finished copying " + this.percent +"%");
		
	}

	public void init(int op, String src, String dest, long max) {
		this.max = max;
		_log.debug("starting copy from source: " + src +" to destination "+ dest + " total file size:  "+ max);
        	
	}

}
