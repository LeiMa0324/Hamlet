package hamlet.workload;

import hamlet.users.stockUser.KleeneEventTypeEnum;
import hamlet.users.stockUser.NoneKleeneEventTypeEnum;
import hamlet.users.stockUser.stockAttributeEnum;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class WorkloadTemplate {

    private HashMap<String, ArrayList<String>> candidateQueries;

    public WorkloadTemplate(){
        this.candidateQueries = new HashMap<>();
    }

    /**
     * generate a workload
     * @param groupNum the number of mini-workloads
     */
    public void generateCandidateQueries( int groupNum){

        //set predicate
        String[] predOneColumns = {stockAttributeEnum.open.toString(),
                                    stockAttributeEnum.close.toString()};
        String[] predTwoColumns = {stockAttributeEnum.high.toString(),
                                    stockAttributeEnum.low.toString()};


        //set groupby
        String groubyColumn = stockAttributeEnum.open_level.toString();

        //set aggregate column & type
        String aggreColumn = stockAttributeEnum.open.toString();


        ArrayList<KleeneEventTypeEnum> availableKleene = new ArrayList<>(Arrays.asList(KleeneEventTypeEnum.values()));
        ArrayList<NoneKleeneEventTypeEnum> availableNoneKleene = new ArrayList<>(Arrays.asList(NoneKleeneEventTypeEnum.values()));

        //setting for mini-workload
        for (int j =0; j < groupNum; j++){

            //set aggregator
            String preAggregator = Math.random()<0.4?"COUNT":"";

            //set shared kleene event

            String kleene = ((KleeneEventTypeEnum) randomEvent(availableKleene)).toString();


            String[] windows = {"5 min", "10 min","15 min", "20 min"};
            String slide = "5 min";

            ArrayList<String> queries = new ArrayList<>();

            for (int i =0; i < 20 ; i++){

                String prefix = randomPrefix(availableNoneKleene);
                String suffix = randomSuffix(availableNoneKleene);

                String pattern = prefix+ kleene+"+"+suffix;

                String aggregator = preAggregator.equals("")?randomAggregator():preAggregator;
                String returnString = returnString(kleene, groubyColumn, aggreColumn, aggregator);
                String patternString = patternString(pattern);

                String operator = ">";
                String predCol = "vol";

                double dice = Math.random();
                int threshold = dice<0.3?100:(dice<0.6? 50: 30);

                String predicate = kleene+"."+predCol+" "+operator+" "+threshold;

                String whereString = whereString(kleene, predicate, operator, groubyColumn);

                String windowString = windowString(randomWindow(windows), slide);

                String query = returnString+"\n"+
                        patternString+"\n"+
                        whereString+"\n"+
                        windowString;

                queries.add(query);
            }
            this.candidateQueries.put(kleene, queries);
        }
    }

    public void generateWorkload(int workloadSize, String WorkloadFile){
        int queryPerGroup = workloadSize/5;
        ArrayList<String> queries = new ArrayList<>();



        for (String kleene: this.candidateQueries.keySet()){

            ArrayList<String> availableQueries = (ArrayList<String>) this.candidateQueries.get(kleene).clone();

            for (int i=0; i<queryPerGroup; i++){

                Random random = new Random();
                int randomIndex = random.nextInt(availableQueries.size());
                queries.add(availableQueries.get(randomIndex));
                availableQueries.remove(randomIndex);
            }
        }

        ArrayList<String> shuffledQueries = shuffle(queries);
        toFile(WorkloadFile, shuffledQueries);

    }

    private String randomPrefix(ArrayList<NoneKleeneEventTypeEnum> availableNoneKleene){
        double prefixRandom = Math.random();
        return prefixRandom > 0.5? randomEvent(availableNoneKleene).toString()+", ":"";

    }

    private String randomSuffix(ArrayList<NoneKleeneEventTypeEnum> availableNoneKleene){
        double suffixRandom = Math.random();
        return suffixRandom > 0.5? ", "+randomEvent(availableNoneKleene).toString():"";

    }

    private String randomAggregator(){
        double random = Math.random();
        return random <0.5? "SUM":"AVG";
    }

    private String randomWindow(String[] windows){
        Random r = new Random();
        return windows[r.nextInt(windows.length)];
    }


    private String returnString(String kleene, String groubyColumn, String aggreColumn, String aggregator){

        String aggregationString = aggregator.equals("COUNT")?"COUNT(*)":aggregator+"("+kleene+"."+aggreColumn+")";
        return "RETURN "+kleene+"."+groubyColumn+", "+aggregationString;
    }

    private String patternString(String pattern){
        String[] events = pattern.split(",");
        return events.length==1 ? "PATTERN "+ pattern : "PATTERN SEQ("+pattern+")";
    }

    private String whereString(String kleene, String predicate,String operator, String groubyColumn){


        return "WHERE "+predicate+"\n"+
                "GROUP-BY " +kleene+"."+groubyColumn;
    }

    private String windowString(String window, String sliding){
        return "WITHIN "+ window+" SLIDE " +sliding;
    }

    /**
     * return a random event from an arraylist and remove it
     * @param array
     * @return
     */
    private<T> T randomEvent(ArrayList<T> array){

        Random r = new Random();
        int randomIndex = r.nextInt(array.size());
        T obj = array.get(randomIndex);
        array.remove(randomIndex);
        return obj;

    }

    public ArrayList<String> shuffle(ArrayList<String> queries){
        Collections.shuffle(queries);
        return queries;
    }

    /**
     * write the queries into a workload file
     */
    public void toFile(String workloadFile, ArrayList<String> queries){

        try{
            File output_file = new File(workloadFile);
            if (!output_file.exists()){
                output_file.createNewFile();
            }
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int i = 0;
            for(String q: queries){
                output.append("q"+i+"\n");
                output.append(q+"\n\n");
                i++;
            }
            output.close();
        }catch (IOException e) { e.printStackTrace(); }


    }
}
