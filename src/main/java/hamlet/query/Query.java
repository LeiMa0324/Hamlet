package hamlet.query;

import hamlet.query.aggregator.Aggregator;
import hamlet.query.pattern.Pattern;
import hamlet.query.predicate.Predicate;
import hamlet.query.window.Window;
import lombok.Data;

import java.util.ArrayList;

/**
 * class of queries
 */
@Data
public class Query {
    protected Pattern pattern;
    protected ArrayList<Predicate> predicates;
    protected Aggregator aggregator;
    protected GroupBy groupBy;
    protected Window window;


    public Query(Pattern pattern,
                 ArrayList<Predicate> predicates,
                 Aggregator aggregator,
                 GroupBy groupBy,
                 Window window){
        this.pattern = pattern;
        this.predicates = predicates;
        this.aggregator = aggregator;
        this.groupBy = groupBy;
        this.window = window;
    }


    @Override
    public String toString() {

        StringBuilder pattern = new StringBuilder(this.pattern.toString());


        StringBuilder conditions = new StringBuilder("");
        for (Predicate p: this.predicates){
            conditions.append(p.toString());
            if (this.predicates.indexOf(p)!=predicates.size()-1){
                conditions.append(" AND");
            }
        }

        return "RETURN "+this.groupBy.getAttributeName() +", "+ this.aggregator.toString()+"\n"+
                "PATTERN "+pattern.toString()+"\n"+
                "WHERE "+conditions.toString()+"\n"+
                "GROUP-BY "+groupBy.getAttributeName()+"\n"+
                window.toString();

    }
}

