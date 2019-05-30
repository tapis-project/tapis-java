package edu.utexas.tacc.aloe.shared.uuid;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"unit"})
public class AgaveUUIDTest {
	private static final Logger _log = LoggerFactory.getLogger(AgaveUUIDTest.class);
    
    @Test
    public void getUniqueId() throws InterruptedException 
    {
        int count = 200000;
        HashSet<String> uuids = new HashSet<String>();
        for (int i = 0; i < count; i++) {
        	AloeUUID uuid = new AloeUUID(UUIDType.FILE);
            String suuid = uuid.toString();
            Assert.assertTrue(uuids.add(suuid), "Duplicate UUID " + suuid
                    + " was created.");
            if (i%10000 == 0) {
            	_log.debug("["+i+"] UUID generated");
            }
        }
        _log.debug(count + " uuid generated without conflict");
    }
}
