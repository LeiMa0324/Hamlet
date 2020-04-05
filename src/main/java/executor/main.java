package executor;

public class main {
    public static void main(String[] args)
    {
        int expNo = 4;

        /**
         * Syntehtic dataset
         */

          //baseline experiments

        Experiment syn_baseExp = new Experiment(true,true, expNo);
        syn_baseExp.varyEventsPerWindow();
        syn_baseExp.varyNumOfSharedEvents();
        syn_baseExp.varyNumofQueries();
        expNo++;

        //hamelet and greta experiments
//        Experiment syn_hamExp = new Experiment(true,false, expNo);
//        syn_hamExp.varyEventsPerWindow();
//        syn_hamExp.varyNumofQueries();
//        syn_hamExp.varyNumOfSharedEvents();
//        syn_hamExp.varyNumofQueries();
//        expNo++;

        /**
         * NYC Taxi dataset
         */


        //hamelet and greta experiments
//        Experiment nyc_hamExp = new Experiment(false,false, expNo);
//        nyc_hamExp.varyEventsPerWindow();
//        nyc_hamExp.varyNumofQueries();



    }
}
