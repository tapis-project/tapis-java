package edu.utexas.tacc.tapis.systems;

import com.google.inject.AbstractModule;

/*
 * Guice moduel for SystemsLib
 */
public class SystemsLibModule extends AbstractModule
{
  @Override
  protected void configure()
  {
//      bind(TransactionLog.class).to(DatabaseTransactionLog.class);

      /*
       * Similarly, this binding tells Guice that when CreditCardProcessor is used in
       * a dependency, that should be satisfied with a PaypalCreditCardProcessor.
       */
//    bind(CreditCardProcessor.class).to(PaypalCreditCardProcessor.class);
  }
}
