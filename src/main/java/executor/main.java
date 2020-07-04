package executor;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;
import java.util.Scanner;

public class main {
    public static void main(String[] args)
    {
        int expNo = 0;
        String expNoFile = "ExpNo.txt";
        // read Experiment Number
        try {

            Scanner scanner = new Scanner(new File(expNoFile));
            expNo= scanner.nextInt();
            System.out.println("Experiment No: " +expNo);

        }catch (FileNotFoundException e){

        }


        boolean islocal = true;
        System.out.println("Running on local or Jar?(0:local, 1: Jar):");
        Scanner in = new Scanner(System.in);

        String s = in.nextLine();
        islocal = s.equals("0");

        System.out.print("Select experiment(0: Synthetic baseline, 1: Synthetic hamlet and greta, 2: NYC taxi," +
                " 3: Smart Home):\n");
        String e = in.nextLine();

        String sube = "";
        if (e.equals("0")||e.equals("1")){
            System.out.print("Select synthetic sub experiment(0: all, 1: vary epw, 2: vary Num of shared, 3: vary number of queries):\n");
            sube = in.nextLine();
        }else {
            System.out.print("Select sub experiment(0: all, 1: vary epw, 2: vary number of queries):\n");
            sube = in.nextLine();
        }


        System.out.println("Runing: " + e);
        switch (e){

            case "0":
                /**
                 * Syntehtic dataset
                 */
                //baseline experiments

                Experiment syn_baseExp = new Experiment(0,true, expNo, islocal);
                switch (sube){
                    case "0":
                        syn_baseExp.varyEventsPerWindow();
                        syn_baseExp.varyNumOfSharedEvents();
                        syn_baseExp.varyNumofQueries();
                        break;
                    case "1":
                        syn_baseExp.varyEventsPerWindow();
                        break;
                    case "2":
                        syn_baseExp.varyNumOfSharedEvents();
                        break;
                    case "3":
                        syn_baseExp.varyNumofQueries();
                        break;

                }
                break;


            case "1":
                //hamelet and greta experiments
                Experiment syn_hamExp = new Experiment(0,false, expNo, islocal);
                switch (sube){
                    case "0":
                        syn_hamExp.varyEventsPerWindow();
                        syn_hamExp.varyNumOfSharedEvents();
                        syn_hamExp.varyNumofQueries();
                        break;
                    case "1":
                        syn_hamExp.varyEventsPerWindow();
                        break;
                    case "2":
                        syn_hamExp.varyNumOfSharedEvents();
                        break;
                    case "3":
                        syn_hamExp.varyNumofQueries();
                        break;
                }
                break;
            case "2":
                /**
                 * NYC Taxi dataset
                 */

                //hamelet and greta experiments
                Experiment nyc_hamExp = new Experiment(1,false, expNo, islocal);
                switch (sube){
                    case "0":
                        nyc_hamExp.varyEventsPerWindow();
                        nyc_hamExp.varyNumofQueries();
                        break;
                    case "1":
                        nyc_hamExp.varyEventsPerWindow();
                        break;
                    case "2":
                        nyc_hamExp.varyNumofQueries();
                        break;

                }

                break;
            case "3":
                /**
                 * Smart Home dataset
                 */

                //hamelet and greta experiments
                Experiment sh_hamExp = new Experiment(2,false, expNo, islocal);
                switch (sube){
                    case "0":
                        sh_hamExp.varyEventsPerWindow();
                        sh_hamExp.varyNumofQueries();
                        break;
                    case "1":
                        sh_hamExp.varyEventsPerWindow();
                        break;
                    case "2":
                        sh_hamExp.varyNumofQueries();
                        break;

                }

                break;

        }

        try{
            expNo++;
            FileWriter writer = new FileWriter(new File(expNoFile));
            writer.write(expNo+"");
            writer.close();

        }catch (IOException excep){

        }




    }
}
