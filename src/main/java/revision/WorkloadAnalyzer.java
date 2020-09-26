package revision;

import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * this workload analyzer analyzes the sharing opportunity of a multi-query workload
 * it reads a workload file, turns it into a list of queries and
 * decides which queries could be shared together and partition the queries into mini-workloads
 */
@Data
public class WorkloadAnalyzer {


    // a list of mini-workloads
    //<event type: mini-workload>
    private HashMap<Integer, Workload> miniWorkloads;

    public WorkloadAnalyzer(){
        this.miniWorkloads= new HashMap<>();

    }

    /**
     * read queries from the workload file
     * @param workloadFile the workload file
     */
    public void fromFile(String workloadFile){
        try{
            Scanner query_scanner = new Scanner(new File(workloadFile));
            while (query_scanner.hasNextLine()) {

                int eventType=0;
                int predicate=0;
                String aggreFunc="";
                int window=0;

                // for every six lines
                for (int i = 0; i < 6; i++) {
                    String line = query_scanner.nextLine();
                    switch (i){
                        case 0:
                            continue;
                        case 1:
                            String[] lines = line.split(",");

                            //aggregation function
                            aggreFunc = lines[1].trim();
                            String pa = "(RETURN\\s)(\\d*)";
                            Pattern p = Pattern.compile(pa);
                            Matcher a = p.matcher(lines[0]);

                            //vendorID
                            if (a.find()){
                                eventType = Integer.parseInt(a.group(2));

                            }
                            break;
                        case 3:
                            String pattern = ".*trip_distance>([0-9]).*";
                            Pattern r = Pattern.compile(pattern);
                            Matcher m = r.matcher(line);

                            //trip distance
                            if (m.find()){
                                predicate = Integer.parseInt(m.group(1));

                            }
                            break;
                        case 4:
                            String pattern2 = "(WITHIN\\s)(\\d*)(\\smin)";
                            Pattern re = Pattern.compile(pattern2);

                            Matcher ma = re.matcher(line);

                            // the window
                            if (ma.find()){
                                window = Integer.parseInt(ma.group(2));

                            }
                            break;
                    }
                }

                //create a new query
                Query q = new Query(eventType , predicate, aggreFunc, window);
                toMiniWorkload(q);

            }
            query_scanner.close();
        }catch (IOException e){

        }
    }

    /**
     * group the query with queries that have the same:
     * event type
     * aggregation function
     * predicates
     * queries
     */
    public void toMiniWorkload(Query q){

        Workload mini ;

        if (!miniWorkloads.keySet().contains(q.getSharedET())){
            ArrayList<Query> queries = new ArrayList<>();
            mini = new Workload(queries);
        }else {
            mini = miniWorkloads.get(q.getSharedET());
        }
        mini.addQuery(q);
        miniWorkloads.put(q.getSharedET(), mini);


    }
}
