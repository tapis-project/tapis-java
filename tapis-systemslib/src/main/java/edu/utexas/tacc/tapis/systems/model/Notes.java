package edu.utexas.tacc.tapis.systems.model;

/*
 * Class representing simple unstructured metadata formatted as json.
 * Immutable
 * Please keep it immutable.
 *
 */
public final class Notes
{
  private final String data;
  public Notes(String d) { data = d; }
  public String getData() { return data; }
}
