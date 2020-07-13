package executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class main {
    public static void main(String[] args){


        int expNo = 0;

        String expNoFile = "ExpNo.txt";
        // read Experiment Number from file
        try {

            Scanner scanner = new Scanner(new File(expNoFile));
            expNo= scanner.nextInt();
            System.out.println("Experiment No: " +expNo);

        }catch (FileNotFoundException e){
        }

        /**
         * Hamlet versus state-of-the-art approaches
         * Ride sharing dataset
         * Figure 14
         */
        Experiment rideSharing_Exp = new Experiment(0,true, expNo, true);

        //vary events per window
        rideSharing_Exp.varyEventsPerWindow();

        //vary number of queries
        rideSharing_Exp.varyNumofQueries();


        /**
         * Dynamic versus static sharing decision
         * Ride sharing dataset
         * Figure 17
         */

        Experiment decision_Exp = new Experiment(0,false, expNo, true);

        //vary events per window
        decision_Exp.Dynamic_varyEPW();

        //vary burst size
        decision_Exp.Dynamic_varyBurstSize();


        /**
         * update the experiment number in the file
         */
        try{
            expNo++;
            FileWriter writer = new FileWriter(new File(expNoFile));
            writer.write(expNo+"");
            writer.close();

        }catch (IOException excep){

        }
    }
}
