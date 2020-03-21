package edu.utexas.tacc.tapis.security.commands.model;

/** Base class for all secrets that maintains outcomes 
 * as each secret is processed.
 * 
 * @author rcardone
 */
public abstract class SkAdminAbstractSecret 
{
    public boolean created;
    public boolean updated;
    public boolean deployed;
    public boolean failed;
}
