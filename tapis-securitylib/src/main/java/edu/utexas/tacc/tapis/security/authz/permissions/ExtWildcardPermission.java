package edu.utexas.tacc.tapis.security.authz.permissions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.util.CollectionUtils;

/** This class allows predefined schemas to extend standard Shiro permission
 * checking.  For example, one extension incorporates pathnames or URLs into 
 * permission specifications and provides custom matching semantics for those values.
 * 
 * Each colon separated segment of a permission string is called a part.  Custom 
 * schemas can define one or more parts that have their own matching semantics and
 * syntactic rule.  One consideration when specifying a custom schema is to make
 * sure it is well-defined.  In particular, any schema with a custom part that might
 * contain a Shiro permissions reserved character (:, *, comma) can only be the last
 * part of the schema.  Without this constraint permission parsing would become 
 * ambiguous. 
 * 
 * We continue the coding practices of the Shiro super class with regard to exception
 * handling and logging, which is a different that typical SK code.  Some code was
 * copied directly from Apache Shiro.
 * 
 * @author rcardone
 *
 */
public final class ExtWildcardPermission 
 extends WildcardPermission
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final long serialVersionUID = -673253989964825349L;

    /* **************************************************************************** */
    /*                                    Enums                                     */
    /* **************************************************************************** */
    /* This enum represents the identifiers for all supported extended matching types.  
     * 
     * Types that begin with an underscore indicate that their values may contain Shiro 
     * permissions reserved characters (:, *, comma).  We say these types are NOT 
     * Shiro-parsing safe.  The values of these types have to be parsed differently and
     * cannot be compounded, i.e., commas are not recognized as value separators within 
     * such a part.
     * 
     * In addition, only one Shiro-parsing unsafe type can be defined in a schema and
     * that type can only be specified as the last part in the schema.  The supported
     * matching types are listed here: 
     *
     * SHIRO
     * -----
     * Semantics: Apply standard Shiro matching.
     *
     * _RECURSIVE_PATH
     * ---------------
     * Example schema: files:tenantId:op:systemId:path
     * Shiro-parsing safe: No
     * Semantics: Path matching where the :path means the named path and everything in
     *            its subtree.  Matches require that either the path is an exact match 
     *            or the (path + "/") is a prefix of the request path.   
     */
    private enum ExtMatchType {SHIRO, _RECURSIVE_PATH}
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // A mapping of strings to pairs of matching attributes using a custom type
    // representation.  The string key represents the schema (the first segment) of a 
    // permission.  Each pair associated with that schema consists of the the match 
    // type and the segment ordinal in which the path appears (left to right,
    // starting at 0).
    private static final ExtMatchInfo[] _extMatchInfo = initExtMatchInfo();
    
    // Array of match types where the position of each type corresponds to the schema 
    // position to which that matching type is applied.  Null means this is not a 
    // extended schema.
    private ExtMatchType[] _typeArray;

    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ExtWildcardPermission(String wildcardString) {
        super(wildcardString, DEFAULT_CASE_SENSITIVE);
    }

    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ExtWildcardPermission(String wildcardString, boolean caseSensitive) {
        super(wildcardString, caseSensitive);
    }
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getRecursivePathIndex:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Return the index of the extended path part in the specified schema.  If not
     * found because the schema is not extended, or if the schema does not define
     * an extended path part in its last position, return -1.
     * 
     * @param schema the schema name, such as "files"
     * @return the path part's index in the schema definition
     */
    public static int getRecursivePathIndex(String schema)
    {
        // Find the extended schema information in the static array that
        // contains all supported extensions.  If the schema is found and
        // it has a part defined to be a path, return the path's index.
        for (var info : _extMatchInfo) 
            if (schema.equals(info._schema))
                for (var pair : info._extensions)
                    if (pair.getLeft() == ExtMatchType._RECURSIVE_PATH)
                        return pair.getRight();
        
        // Schema with path not found.
        return -1;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* equals:                                                                      */
    /* ---------------------------------------------------------------------------- */
    @Override
    public boolean equals(Object o) 
    {
        // Use the superclass equality test for all instances of this class. 
        if (!(o instanceof ExtWildcardPermission)) return false;
        return super.equals(o);
    }

    /* **************************************************************************** */
    /*                             Protected Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* setParts:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Initial parsing of permission strings.  In keeping with the original Shiro 
     * implementation, runtime exceptions can be thrown from here.
     * 
     * @param wildcardString the permission string
     * @param caseSensitive whether the permission matching will be case sensitive 
     */
    @Override
    protected void setParts(String wildcardString, boolean caseSensitive) 
    {
        // Use the same error message as the superclass.
        if (StringUtils.isBlank(wildcardString)) {
            throw new IllegalArgumentException(
                    "Wildcard string cannot be null or empty. "
                    + "Make sure permission strings are properly formatted.");
        }
        
        // Construct the schema's type map.
        _typeArray = getTypeArray(wildcardString);
        
        // Let the super class process standard shiro permission parsing.
        if (_typeArray == null) {
            super.setParts(wildcardString, caseSensitive);
            return;
        }
        
        // --------------- Extended Permission Processing ---------------
        // A somewhat funny way to deal with case sensitivity carried over from Shiro.
        if (!caseSensitive) wildcardString = wildcardString.toLowerCase();
        
        // This is a custom schema with one or more extensions.  Determine if
        // the last component is a Shiro-parsing unsafe type and, if so, cut
        // off parsing by limiting the list length.  This limit has the effect
        // of not splitting the last value.
        boolean isShiroParsingSafe = isShiroParsingSafe(); 
        int splitParts = isShiroParsingSafe ? 0 : _typeArray.length; // 0 means no limit
        List<String> parts = 
            CollectionUtils.asList(wildcardString.split(PART_DIVIDER_TOKEN, splitParts));
        
        // Create the list for the decomposed parts.
        var nestedParts = new ArrayList<Set<String>>();
        
        // Decompose each comma separated value.
        int lastPartIndex = _typeArray.length - 1; // custom schema max index
        for (int i = 0; i < parts.size(); i++) 
        {
            // Current part.
            String part = parts.get(i);
            
            // Set for the comma separated strings in the current part.
            Set<String> subparts;
            
            // Handle the non-parseable custom value which 
            // can only be in the last position.
            if (!isShiroParsingSafe && i == lastPartIndex) 
                subparts = CollectionUtils.asSet(new String[] {part});
            else 
                subparts = CollectionUtils.asSet(part.split(SUBPART_DIVIDER_TOKEN));

            // Check subparts before adding to nested part list.
            if (subparts.isEmpty()) 
                throw new IllegalArgumentException(
                        "Wildcard string cannot contain parts with only dividers. "
                        + "Make sure permission strings are properly formatted.");
            
            nestedParts.add(subparts);
        }
        
        // Make sure we have a non-empty permission spec.
        if (nestedParts.isEmpty()) 
            throw new IllegalArgumentException("Wildcard string cannot contain only dividers. "
                    + "Make sure permission strings are properly formatted.");

        // Assign the parent field.
        super.setParts(nestedParts);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* implies:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** The basic logic is the same as in standard Shiro except for the actual part
     * comparison which depends on the matching type.
     * 
     * @param p the request permission string
     * @return true if the request permission is implied by this permission, false otherwise
     */
    @Override
    public boolean implies(Permission p)
    {
        // Use standard shiro matching when this object is not extended.
        if (!requiresExtendedParsing()) return super.implies(p);
        
        // We only compare to ourselves.
        if (!(p instanceof ExtWildcardPermission)) return false;
        ExtWildcardPermission otherPerm = (ExtWildcardPermission) p;
        List<Set<String>> otherParts = otherPerm.getParts();
        
        // First iterate through the otherPerm's parts. 
        int i = 0;
        for (Set<String> otherPart : otherParts) {
            // If this permission has less parts than the other permission, 
            // everything after the number of parts contained in this 
            // permission is automatically implied, so return true.
            if (getParts().size() - 1 < i) return true;
            
            // Check this position for a match.  Note the containsAll call
            // means that comma is interpreted as AND in either permission.
            Set<String> part = getParts().get(i);
            if (!impliesPart(_typeArray[i], part, otherPart)) 
                return false;
            
            // Move to the next position.
            i++;
        }

        // If this permission has more parts than the other permission, only imply 
        // the other permission if all of the excess parts are wildcards.
        for (; i < getParts().size(); i++) {
            Set<String> part = getParts().get(i);
            if (!part.contains(WILDCARD_TOKEN)) {
                return false;
            }
        }

        // We have a match if we get here.
        return true;
    }
    
    /* **************************************************************************** */
    /*                              Private Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* impliesPart:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Implement the implies logic for each matching type, which includes the standard 
     * Shiro matching and all extended matching semantics.
     * 
     * @param matchType the type of matching to be applied
     * @param part this permission's part
     * @param otherPart the request permission part
     * @return true if there's a match, false otherwise
     */
    private boolean impliesPart(ExtMatchType matchType, Set<String> part, Set<String> otherPart)
    {
        // The match type determines the how we compare the other part to this part.
        switch (matchType) {
            // Standard Shiro matching.
            case SHIRO:
                if (part.contains(WILDCARD_TOKEN) || part.containsAll(otherPart)) 
                    return true;
                break;
                
            // Filepath matching does not recognize any special characters and only
            // matches if (1) the two parts are exactly the same or (2) the other
            // part is a descendant of this part in a path hierarchy.  Avoid false
            // capture by appending the path separator when this part is interpreted
            // as a parent directory/container.
            case _RECURSIVE_PATH:
                // There can only be exactly one string in each part.
                String partString = part.iterator().next();
                String otherPartString = otherPart.iterator().next();
                if (otherPartString.equals(partString)) return true;
                if (otherPartString.startsWith(partString + "/")) return true;
                break;
        }
        
        // No match.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getTypeArray:                                                                */
    /* ---------------------------------------------------------------------------- */
    private ExtMatchType[] getTypeArray(String wildcardString)
    {
        // Get the schema if one is specified.
        int index = wildcardString.indexOf(':');
        if (index < 1) return null;
        String schema = wildcardString.substring(0, index);
        
        // See if a custom schema has been defined.
        for (ExtMatchInfo info : _extMatchInfo) 
            if (info._schema.equals(schema)) {
                // Get the maximum part position.
                int maxIndex = info._extensions.stream().
                                max(Comparator.comparing(Pair::getValue)).get().getValue();
                
                // Create and populate result array.
                var array = new ExtMatchType[maxIndex + 1];
                for (int i = 0; i < array.length; i++) array[i] = ExtMatchType.SHIRO;
                for (var pair : info._extensions) array[pair.getValue()] = pair.getKey();
                return array;
            }
        
        // Not an extended schema.
        return null;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isShiroParsingSafe:                                                          */
    /* ---------------------------------------------------------------------------- */
    private boolean isShiroParsingSafe()
    {
       // Parsing safety is determined by the last custom match type that relies
       // on the naming convention explained above.  Non-extended types are always
       // shiro-parsing safe.
       if (!requiresExtendedParsing()) return true;
       if (_typeArray[_typeArray.length-1].name().startsWith("_")) return false;
         else return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* requiresExtendedParsing:                                                     */
    /* ---------------------------------------------------------------------------- */
    private boolean requiresExtendedParsing() {return _typeArray != null;}
    
    /* ---------------------------------------------------------------------------- */
    /* initExtMatchInfo:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** This method defines all the schemas that require some type of customized
     * permission matching treatment.  The current implementation hardcodes these
     * schemas, but we'd like to eventually move these specifications to the database
     * and, possibly, provide a user-facing API.
     * 
     * Each info item must contain at least one type/position pair.  If a type is not
     * Shiro-parsing safe (i.e., it begins with an underscore), it must be associated
     * with the last part of the schema.  This implies that there can only be at most
     * one Shiro-parsing unsafe type per schema. 
     * 
     * @return the schemas with extended permission semantics
     */
    private static ExtMatchInfo[] initExtMatchInfo()
    {
        // Create the list extended match info objects.
        var extMatchList = new ArrayList<ExtMatchInfo>();
        
        // --- files:tenantId:op:systemId:path
        //
        // Extended path handling for the files schema.  The path is always the 
        // fifth and last segment (index == 4) since it is not Shiro-parsing safe.
        var fileMatchInfo = new ExtMatchInfo("files");
        fileMatchInfo._extensions.add(Pair.of(ExtMatchType._RECURSIVE_PATH, 4));
        extMatchList.add(fileMatchInfo);
        
        // Double-check that Shiro-parsign unsafe types only appear in the last 
        // position.  This is for future-proofing...
        validateExtMatchInfoList(extMatchList);
        
        // Return an array.
        var extMatches = new ExtMatchInfo[extMatchList.size()];
        return extMatchList.toArray(extMatches);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateExtMatchInfoList:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Sanity check the schema definitions.
     * 
     * @param extMatchList the list of all match info objects
     */
    private static void validateExtMatchInfoList(ArrayList<ExtMatchInfo> extMatchList)
    {
        // Check each definition.
        for (var info : extMatchList) {
            // Make sure we have a valid schema name.
            if (StringUtils.isBlank(info._schema)) {
                String msg = "Invalid schema name in extended type definition.";
                throw new IllegalArgumentException(msg);
            }
            
            // The first part of a custom permission is a constant name string.
            if (info._schema.contains("*") || info._schema.contains(",") || 
                info._schema.contains(":")) 
            {
                String msg = "The first position of custom schema " + info._schema + 
                             " must be a constant string without colons, asterisks or commas.";
                throw new IllegalArgumentException(msg);
            }
            
            // Make sure we have at least one extended type.
            if (info._extensions.isEmpty()) {
                String msg = "No extended types defined for custom schema " + info._schema + ".";
                throw new IllegalArgumentException(msg);
            }
            
            // The first part of a custom permission is a constant string name
            // that can only use standard Shiro matching.  Note that the extensions
            // are not ordered.
            int unsafeCount = 0;
            int maxPartIndex = -1;
            int maxUnsafePartIndex = -1;
            for (var pair : info._extensions) {
                // Make sure the first part use standard matching.
                if (pair.getValue() == 0 && pair.getKey() != ExtMatchType.SHIRO) {
                    String msg = "The first part of custom schema " + info._schema + 
                                 " must use standard SHIRO parsing.";
                    throw new IllegalArgumentException(msg);
                }
                
                // Count the number of shiro-parsing unsafe match types specified. 
                String typeName = pair.getKey().name();
                if (typeName.charAt(0) == '_') {
                    unsafeCount++;
                    maxUnsafePartIndex = Math.max(maxUnsafePartIndex, pair.getValue());
                }
                
                // Record the max index.
                maxPartIndex = Math.max(maxPartIndex, pair.getValue());
            }
            
            // Is the definition unparseable because of multiple unsafe match types?
            if (unsafeCount > 1) {
                String msg = "Schema " + info._schema + 
                             " cannot define more than one unsafe parsing extended type.";
                throw new IllegalArgumentException(msg);
            }
            
            // Is the definition unparseable because the unsafe match type 
            // is not in the last position in the schema.
            if (unsafeCount > 0 && (maxPartIndex != maxUnsafePartIndex)) {
                String msg = "Schema " + info._schema + 
                             " defines part at index " + maxPartIndex +
                             " after unsafe parse type at index " + maxUnsafePartIndex + 
                             ". Unsafe parse types can appear only as the last part of a " +
                             " schema definition.";
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    /* **************************************************************************** */
    /*                             ExtMatchInfo Class                               */
    /* **************************************************************************** */
    /** Class that specifies what semantic extensions to apply to which segments in
     * a schema specification.  Each extended schema should have exactly 1 of these
     * records in the array of info objects.
     * 
     * The _schema is the identifying element of the permission; it is always the 
     * in the first position in the schema.  The _extensions is a list of pairs
     * of (ExtMatchType, index in schema).  Only the last position in a schema can
     * have a type that begins with an underscore (a Shiro-parsing unsafe type).
     */
    private static final class ExtMatchInfo
    {
        // A schema can have more than 1 semantically extended segments.
        private final String _schema;
        private final ArrayList<Pair<ExtMatchType,Integer>> _extensions = new ArrayList<>();
        
        // Constructor.
        private ExtMatchInfo(String schema) {_schema = schema;}
    }
}
