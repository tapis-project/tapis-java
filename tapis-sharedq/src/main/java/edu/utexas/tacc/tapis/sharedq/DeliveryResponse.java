package edu.utexas.tacc.tapis.sharedq;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/** Simple container for the results of Consumer.handleDelivery()
 * appropriate for insertion into internal data structures like
 * lists or queues.
 * 
 * @author rcardone
 */
public final class DeliveryResponse 
{
  public String               consumerTag; 
  public Envelope             envelope;
  public AMQP.BasicProperties properties; 
  public byte[]               body;
}
