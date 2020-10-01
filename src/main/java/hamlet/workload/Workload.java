package hamlet.workload;

import hamlet.base.DatasetSchema;
import hamlet.query.Query;
import lombok.Data;

import java.util.ArrayList;

@Data
/**
 * generate the workload
 */
public class Workload {

    private ArrayList<Query> queries;
    private DatasetSchema schema;

    /**
     * the default constructor
     */
    public Workload(DatasetSchema schema){
        this.queries = new ArrayList<>();
        this.schema = schema;
    }


    /**
     * add a query into the workload
     * for workload Analyzer to create a mini-workload
     * @param query a given query
     */
    public void addQuery(Query query){
        this.queries.add(query);
    }

}

