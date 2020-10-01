package hamlet.workload;

import hamlet.base.DatasetSchema;
import hamlet.query.GroupBy;
import hamlet.query.Query;
import hamlet.query.QueryParser;
import hamlet.query.aggregator.Aggregator;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * this workload analyzer analyzes the sharing opportunity of a multi-query workload
 * it reads a workload file, turns it into a list of queries and
 * decides which queries could be shared together and partition the queries into mini-workloads
 */
@Data
public class WorkloadAnalyzer {
    private DatasetSchema schema;

    public WorkloadAnalyzer(DatasetSchema schema) {
        this.schema = schema;
    }

    /**
     * read & parse queries from the workload file
     * @param workloadFile the workload file
     */
    private ArrayList<Query> readQueriesFromFile(String workloadFile) {
        ArrayList<Query> queries = new ArrayList<>();

        try {
            Scanner query_scanner = new Scanner(new File(workloadFile));
            while (query_scanner.hasNextLine()) {

                ArrayList<String> lines = new ArrayList<>();
                for (int i =0; i < 7; i++){
                    lines.add(query_scanner.nextLine());
                }
                QueryParser parser = new QueryParser(this.schema);
                queries.add(parser.parse(lines));

            }
            query_scanner.close();

        }catch(IOException e){}

        return queries;
}

    public HashMap<String, Workload> analyze(String workloadFile){

        ArrayList<Query> queries = readQueriesFromFile(workloadFile);

        HashMap<String, Workload> miniloads = new HashMap<>();

        Workload temp;
        for (Query q: queries){
            String kleene = q.getPattern().getKleeneEventType().getName();
            temp = miniloads.containsKey(kleene)? miniloads.get(kleene): new Workload(schema);
            boolean isSharable = sharingRuleCheck(q, temp);

            if (isSharable){
                temp.addQuery(q);
                miniloads.put(kleene, temp);
            }else {
                System.out.printf("Cannot share this query with any queries!");
            }
        }
        return miniloads;
    }

    /**
     * check if the query has the same kleene event type with the mini workload
     * @param query a given query
     * @param workload a mini workload
     * @return boolean
     */
    private boolean sharingRuleCheck(Query query, Workload workload){

        if (workload.getQueries().isEmpty()){
            return true;
        }

        String givenKleene = query.getPattern().getKleeneEventType().getName();
        GroupBy givenGroupBy = query.getGroupBy();
        Aggregator givenAggreg = query.getAggregator();

        for (Query q: workload.getQueries()){

            String existKleene = q.getPattern().getKleeneEventType().getName();
            GroupBy existGroupBy = q.getGroupBy();
            Aggregator existAggreg = q.getAggregator();

            if (!sameKleeneEventTypes(givenKleene, existKleene)&&sameGroupBy(givenGroupBy, existGroupBy)&&
            compatibleAggregators(givenAggreg, existAggreg)){
                return false;
            }
        }
        return true;
    }

    private boolean sameKleeneEventTypes(String etName1, String etName2){
        return etName1.equals(etName2);
    }

    private boolean sameGroupBy(GroupBy groupBy1, GroupBy groupBy2){

        return groupBy1.getEventType().getName().equals(groupBy2.getEventType().getName())&&(
                groupBy1.getAttributeName().equals(groupBy2.getAttributeName())
                );
    }

    private boolean compatibleAggregators(Aggregator agg1, Aggregator agg2){
        ArrayList<Aggregator.Aggregfunctions> funcs = new ArrayList<>();
        funcs.add(agg1.getFunc());
        funcs.add(agg2.getFunc());
        if (funcs.contains(Aggregator.Aggregfunctions.COUNT) && (funcs.get(0)!=funcs.get(1))){
            return false;
        }
        return agg1.getAttributeName().equals(agg2.getAttributeName());
    }
}
