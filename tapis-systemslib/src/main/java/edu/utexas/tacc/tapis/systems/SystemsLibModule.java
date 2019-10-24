package edu.utexas.tacc.tapis.systems;

import com.google.inject.AbstractModule;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;

/*
 * Guice module for SystemsLib
 */
public class SystemsLibModule extends AbstractModule
{
  @Override
  protected void configure()
  {
    bind(SystemsDao.class).to(SystemsDaoImpl.class);
//      bind(TransactionLog.class).to(DatabaseTransactionLog.class);

      /*
       * Similarly, this binding tells Guice that when CreditCardProcessor is used in
       * a dependency, that should be satisfied with a PaypalCreditCardProcessor.
       */
//    bind(CreditCardProcessor.class).to(PaypalCreditCardProcessor.class);
  }
}
