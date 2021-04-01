package edu.utexas.tacc.tapis.jobs.utils;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

/** Use this test to demonstrate how glob processing works for various inputs.
 * Just run the program and inspect the results.
 * 
 * The approach the uses a glob specification without a leading slash, such as 
 * "** /*" (with no internal space), and path names that begin with slashes
 * seems to give the most intuitive results.
 * 
 * @author rcardone
 */
public class GlobTest 
{
    public static void main(String[] args) 
    {
        String g = "glob:*.txt";
        System.out.println("------- " + g);
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(g);
        
        Path path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        // --------------------
        g = "glob:**/*.txt";
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        // This approach uses a glob without a leading slash but file names
        // with leading slashes seems to be the most intuitive.
        g = "glob:**/*.txt"; 
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        g = "glob:/**/*.txt";  // Not a great idea
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        g = "glob:/**/*.txt"; // Not a great idea
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        g = "glob:/*/*/*/*.txt";
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        g = "glob:**.txt";
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        g = "glob:**.txt";
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        // --------------------
        g = "glob:**/*";
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

        // --------------------
        // This approach uses a glob without a leading slash but file names
        // with leading slashes seems to be the most intuitive.
        g = "glob:**/*"; 
        System.out.println("------- " + g);
        matcher = FileSystems.getDefault().getPathMatcher(g);        
        path = Paths.get("/one/two/three/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));
        
        path = Paths.get("/bud.txt");
        System.out.println(path.toString() + ": " + matcher.matches(path));

    }
}
