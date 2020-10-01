package executor;

import java.io.File;
import java.io.IOException;

public class test {

    public static void main(String args[]) throws IOException {


        /**
         * Hamlet versus state-of-the-art approaches
         * Ride sharing dataset
         * Figure 14
         */
        Experiment rideSharing_Exp = new Experiment(0,true, 10, true);

        //vary events per Window
        rideSharing_Exp.varyEventsPerWindow();

        //vary number of queries
        rideSharing_Exp.varyNumofQueries();

    }

    public static void checkFolder(String path){

        File folder = new File(path);

        if (!folder.exists() && !folder.isDirectory()) {
            folder.mkdirs();
            System.out.println("directory created");
        } else {
            System.out.println();
        }

    }
}
