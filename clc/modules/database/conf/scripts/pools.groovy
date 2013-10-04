import com.eucalyptus.auth.crypto.Hmacs;
import com.eucalyptus.bootstrap.Component;
import org.logicalcobwebs.proxool.ProxoolFacade;
import com.eucalyptus.util.LogUtil;

db_pass = System.getProperty("euca.db.password")!=null && System.getProperty("euca.db.password").length()>1 ? System.getProperty("euca.db.password") : Hmacs.generateSystemSignature( );
ClassLoader.getSystemClassLoader().loadClass('org.logicalcobwebs.proxool.ProxoolDriver');
poolProps = [
  'proxool.simultaneous-build-throttle': '16',
  'proxool.minimum-connection-count': '16',
  'proxool.maximum-connection-count': '128',
  'proxool.house-keeping-test-sql': 'SELECT * FROM COUNTERS;',
  'user': 'postgres',
  'password': '',
]
p = new Properties();
p.putAll(poolProps)
String dbDriver = 'org.postgresql.Driver';
String url = "proxool.eucalyptus_${context_name}:${dbDriver}:jdbc:postgresql://localhost/hippo";
LogUtil.logHeader( "Proxool config for ${context_name}" ).log( url ).log( poolProps )
ProxoolFacade.registerConnectionPool(url, p);

[
  'hibernate.bytecode.use_reflection_optimizer': 'true',
  'hibernate.cglib.use_reflection_optimizer': 'true',
  'hibernate.dialect': 'org.hibernate.dialect.PostgreSQLDialect',
  'hibernate.connection.provider_class': 'org.hibernate.connection.C3P0ConnectionProviders',
  'hibernate.proxool.pool_alias': "eucalyptus_${context_name}",
  'hibernate.proxool.existing_pool': 'true'
]
