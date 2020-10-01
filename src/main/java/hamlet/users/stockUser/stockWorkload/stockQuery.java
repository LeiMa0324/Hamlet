package hamlet.users.stockUser.stockWorkload;

import lombok.Data;

/**
 * 每个query就是x+
 * generate a query, with the clauses of
 * shared kleene event type: 1-10
 * predicate: passenger_count
 * groupby: payment type
 * Window: none
 * aggregation function: count and sum
 */
@Data
public class stockQuery {

    //the query Pattern
    private String pattern ="";

    // the shared event type
    private int sharedET;

    // the event type with predicate
    //predicate on trip_distance
    private int predicateCol = 5;

    private int predicate;

    //VENDOR ID TO GROUPBY
    //group by payment type
    private int groupbyCol = 10;

    //Window
    private int window = 10000;

    //the aggregation function
    //SUM: total_amount
    private String aggregFunc;

    public stockQuery(int sharedET , int predicate, String aggregation){
        this.predicateCol = 5;
        this.groupbyCol = 10;
        this.sharedET = sharedET;
        this.pattern = sharedET+"+";
        this.predicate = predicate;
        this.aggregFunc = aggregation;


    }

    public stockQuery(int sharedET , int predicate, String aggregation, int window){
        this.predicateCol = 5;
        this.groupbyCol = 10;
        this.sharedET = sharedET;
        this.pattern = sharedET+"+";
        this.predicate = predicate;
        this.aggregFunc = aggregation;
        this.window = window;
    }

    @Override
    public String toString(){
        return "RETURN "+ sharedET+", "+this.aggregFunc+"\n"+
                "PATTERN SEQ("+sharedET+"+)"+"\n"+
                "WHERE "+sharedET+".trip_distance>"+this.predicate+" GROUP-BY "+sharedET+".payment_type"+"\n"+
                "WITHIN "+window+" min";

    }

}

