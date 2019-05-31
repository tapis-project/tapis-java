package edu.utexas.tacc.tapis.shared.i18n;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

@Test(groups={"unit"})
public class MessageUtilsTest 
{
	/* **************************************************************************** */
	/*                                    Fields                                    */
	/* **************************************************************************** */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(MessageUtilsTest.class);
	
	/* **************************************************************************** */
	/*                                    Tests                                     */
	/* **************************************************************************** */
	/* --------------------------------------------------------------------------- */
	/* testLog:                                                                    */
	/* --------------------------------------------------------------------------- */
	@Test(enabled=true)
	public void testLog()
	{
		// The output depends on the configuration logback.xml.
		_log.info("info");
		_log.warn("warn");
		_log.error("error");
		_log.debug("debug");
		_log.trace("trace");
	}
	
	/* --------------------------------------------------------------------------- */
	/* testLogMsg:                                                                 */
	/* --------------------------------------------------------------------------- */
	@Test(enabled=true)
	public void testLogMsg()
	{
		// The output depends on the configuration logback.xml.
		_log.info(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", "banana"));
		_log.warn(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", "pear"));
		_log.error(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", "apple"));
		_log.debug(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", "cherry"));
		_log.trace(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", "peach"));
	}
}
