package edu.utexas.tacc.tapis.security.secrets;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups= {"unit"})
public class SecretTypeDetectorTest 
{
    @Test(enabled=true)
    public void detectTypeTest() {
        // DBCredential
        var s = "tapis/service/influx/dbhost/influx_host/dbname/chords_ts_production/dbuser/admin/credentials/passwords";
        Assert.assertEquals(SecretType.DBCredential, SecretTypeDetector.detectType(s));
        
        s = "tapis/service/postgres/dbhost/tapisfiles-postgres/dbname/files/dbuser/files/credentials/passwords";
        Assert.assertEquals(SecretType.DBCredential, SecretTypeDetector.detectType(s));
        
        s = "tapis/service/rabbitmq/dbhost/notifications-rabbitmq/dbname/NotificationsHost/dbuser/notifications/credentials/passwords";
        Assert.assertEquals(SecretType.DBCredential, SecretTypeDetector.detectType(s));
        
        s = "tapis/service/mongo/dbhost/restheart-mongo/dbname/NA/dbuser/restheart/credentials/passwords";
        Assert.assertEquals(SecretType.DBCredential, SecretTypeDetector.detectType(s));
        
        // ServicePwd
        s = "tapis/tenant/admin/service/abaco/kv/password";
        Assert.assertEquals(SecretType.ServicePwd, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/service/files/kv/password";
        Assert.assertEquals(SecretType.ServicePwd, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/service/tenants/kv/password";
        Assert.assertEquals(SecretType.ServicePwd, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/service/meta/kv/password";
        Assert.assertEquals(SecretType.ServicePwd, SecretTypeDetector.detectType(s));
        
        // JWTSigning
        s = "tapis/tenant/a2cps/jwtkey/keys";
        Assert.assertEquals(SecretType.JWTSigning, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/dev/jwtkey/keys";
        Assert.assertEquals(SecretType.JWTSigning, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/jwtkey/keys";
        Assert.assertEquals(SecretType.JWTSigning, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/tacc/jwtkey/keys";
        Assert.assertEquals(SecretType.JWTSigning, SecretTypeDetector.detectType(s));
        
        // User
        s = "tapis/tenant/admin/user/admin/kv/secrets";
        Assert.assertEquals(SecretType.User, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/user/authenticator/kv/ldap.tapis-dev";
        Assert.assertEquals(SecretType.User, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/user/authenticator/kv/ldap.tapis-v2";
        Assert.assertEquals(SecretType.User, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/admin/user/grafana-admin/kv/postgres-ro-password";
        Assert.assertEquals(SecretType.User, SecretTypeDetector.detectType(s));
        
        // System
        s = "tapis/tenant/dev/system/S2-test-testuser2/user/dynamic+testuser2/sshkey/S1";
        Assert.assertEquals(SecretType.System, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/dev/system/gateways-vm-testuser3/user/dynamic+testuser3/password/S1";
        Assert.assertEquals(SecretType.System, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/dev/system/tapisv3-exec2/user/static+testuser2/sshkey/S1";
        Assert.assertEquals(SecretType.System, SecretTypeDetector.detectType(s));
        
        s = "tapis/tenant/dev/system/tapisv3-smoketest-storage-linux/user/static+testuser2/sshkey/S1";
        Assert.assertEquals(SecretType.System, SecretTypeDetector.detectType(s));
    }
}
