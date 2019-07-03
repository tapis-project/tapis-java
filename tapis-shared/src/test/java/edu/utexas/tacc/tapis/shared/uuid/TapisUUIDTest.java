package edu.utexas.tacc.tapis.shared.uuid;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

@Test(groups={"unit"})
public class TapisUUIDTest {
	private static final Logger _log = LoggerFactory.getLogger(TapisUUIDTest.class);
    
    @Test(enabled = false)
    public void getUniqueId() throws InterruptedException 
    {
        int count = 200000;
        HashSet<String> uuids = new HashSet<String>();
        for (int i = 0; i < count; i++) {
        	TapisUUID uuid = new TapisUUID(UUIDType.FILE);
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
