package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;

public final class DatabaseTester 
extends AbsTester
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(DatabaseTester.class);
   
   /* **************************************************************************** */
   /*                                 Constructors                                 */
   /* **************************************************************************** */
   /* ---------------------------------------------------------------------------- */
   /* constructor:                                                                 */
   /* ---------------------------------------------------------------------------- */
   public DatabaseTester(JobRecovery jobRecovery)
   {
       super(jobRecovery);
   }
   
   /* ********************************************************************** */
   /*                             Public Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* canUnblock:                                                            */
   /* ---------------------------------------------------------------------- */
   @Override
   public int canUnblock(Map<String, String> testerParameters) 
   {
       // Not implemented yet.
       return NO_RESUBMIT_BATCHSIZE;
   }
}
