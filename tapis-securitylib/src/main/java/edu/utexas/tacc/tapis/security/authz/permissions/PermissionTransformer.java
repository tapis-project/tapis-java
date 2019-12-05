package edu.utexas.tacc.tapis.security.authz.permissions;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.model.SkRolePermissionShort;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class PermissionTransformer 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(PermissionTransformer.class);
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Initialization fields.
    private final int    _startPartIndex;
    private final String _oldText;
    private final String _newText;
    
    // The result list.
    private ArrayList<Transformation> _transformations;
    
    /* **************************************************************************** */
    /*                             Constructors                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Provide the substitution position, the old text that will be replaced and the
     * new replacement text.  The startPartIndex is the 0-origin index of the part
     * that will be replaced.  For example, in the schema files:tenant:op:system:path
     * the system and a prefix of the path will be replaced, so the startPartIndex is
     * 3, the index of the system part.
     * 
     * The oldText is the old system and search path prefix in the form "oldSystem:oldPrefix",
     * colon include.  Similarly, the newText has the form "newSystem:newPrefix".
     * 
     * @param startPartIndex that index of the first part whose text will be replaced
     * @param oldText the text to be replaced including the colon separator
     * @param newText the replacement text including the colon separator
     */
    public PermissionTransformer(int startPartIndex, String oldText, String newText) 
    {
        _startPartIndex = startPartIndex;
        _oldText        = oldText;
        _newText        = newText;
    }
    
    /* **************************************************************************** */
    /*                             Public Methods                                   */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getTransformations:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Get the previously added transformations.
     * 
     * @return the transformation list which can be null if no permission records 
     *         were added
     */
    public List<Transformation> getTransformations(){return _transformations;}
    
    /* ---------------------------------------------------------------------------- */
    /* getTransformations:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** The input is the list of short permission records that matched the search 
     * criteria and contain the oldText at the startPartIndex.  The output is written
     * to the transformations field which is externally accessible.  
     * 
     * This method can be called multiple times to accumulate transformations.  The 
     * order in which the input records are received is preserved in the transformations 
     * list.
     * 
     * @param perms list of permissions containing oldText
     */
    public void addTransformations(List<SkRolePermissionShort> perms)
    {
        // Don't put up with nonsense.
        if (perms == null || perms.isEmpty()) return;
        
        // Initialize the transformations field if necessary.
        if (_transformations == null) _transformations = new ArrayList<>(perms.size());
        
        // Squirrel away the length of the text to be replaced.
        int oldTextLen = _oldText.length();
        
        // Create each transformation.
        for (var perm : perms) {
            
            // Get the index of the last colon before 
            // the text to be replaced begins.
            int lastPreservedColonIndex = 
                StringUtils.ordinalIndexOf(perm.getPermission(), ":", _startPartIndex);
            
            // The original permission should have the expected number of colons
            // preceding the text to be replaced. If not, we probably have a code
            // problem.
            if (lastPreservedColonIndex < 0) {
                String msg = MsgUtils.getMsg("SK_PERM_PREFIX_NOT_FOUND", 
                                      perm.getPermission(), _oldText, _startPartIndex);
                _log.error(msg);
                continue;
            }
            
            // Initialize the new permission text to include the last colon.
            String newPerm = perm.getPermission().
                               substring(0, lastPreservedColonIndex + 1);

            // Add the new prefix text.
            newPerm += _newText;
            
            // Add the suffix of the original permission.
            newPerm += perm.getPermission().
                         substring(lastPreservedColonIndex + 1 + oldTextLen);
            
            // Create the transformation object.
            _transformations.add(new Transformation(perm.getId(), perm.getPermission(), 
                                                    newPerm));
        }
    }
    
    /* **************************************************************************** */
    /*                          Transformation Class                                */
    /* **************************************************************************** */
    public static final class Transformation
    {
        public Transformation(int id, String o, String n) 
        {permId = id; oldPerm = o; newPerm = n;}
        
        public int    permId;   // the permission record id 
        public String oldPerm;  // the old permission text
        public String newPerm;  // the new permission text
    }
}
