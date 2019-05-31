package edu.utexas.tacc.tapis.shared.utils;

import org.apache.commons.lang3.builder.RecursiveToStringStyle;

/** This class uses the facilities of its superclass to recurse through
 * a nested object graph but does not include the object hashcodes.  This
 * allows two distinct objects that have the same state to be considered
 * equal.
 * 
 * @author rcardone
 *
 */
public class ComparableRecursiveToStringStyle
  extends RecursiveToStringStyle
{
  private static final long serialVersionUID = -6732068351736629772L;
  
  public ComparableRecursiveToStringStyle()
  {
    super();
    
    // Allows different objects of same type with 
    // same printable state to be considered equal.
    this.setUseIdentityHashCode(false);
  }
}
