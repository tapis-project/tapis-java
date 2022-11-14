package edu.utexas.tacc.tapis.misc.taccstats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/** Replace the backslash/newline sequence found in the tacc_stats exported csv files.
 * The original file is unchanged and a new file with the same name plus ".reduced" 
 * is appended.  
 * 
 * @author rcardone
 *
 */
public class ReplaceHexLines {

    public static void main(String[] args) throws IOException 
    {
        // Pattern to be replaced.
        final String target = "\\\n";
        
        // Get the file pathname.
        if (args.length < 1) {
            System.out.println("Command format: ReplaceHexLines <pathname>");
            return;
        }
        
        // Open the output file
        var writer = new BufferedWriter(new FileWriter(args[0] + ".reduced"));
        
        // Open the input file, read each line, and write the modified line out.
        var reader = new BufferedReader(new FileReader(args[0]));
        
        // Read characters into a buffer and remove the offending sequence.
        var cbuf = new char[8192];
        int charsRead = reader.read(cbuf);
        while (charsRead > 0 ) {
            var s = new String(cbuf, 0, charsRead);
            s = s.replace(target, "");
            writer.write(s);
            charsRead = reader.read(cbuf);
        }
        reader.close();
        writer.close();
    }
}
