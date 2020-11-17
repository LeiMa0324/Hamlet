package hamlet.query;

import hamlet.base.DatasetSchema;
import hamlet.base.EventType;
import hamlet.query.aggregator.Aggregator;
import hamlet.query.aggregator.AvgAndSumAggregator;
import hamlet.query.aggregator.CountAggregator;
import hamlet.query.pattern.Pattern;
import hamlet.query.predicate.AttributeComparisonPredicate;
import hamlet.query.predicate.Predicate;
import hamlet.query.predicate.SingleValuePredicate;
import hamlet.query.window.Window;

import java.util.ArrayList;
import java.util.Set;
import java.util.regex.Matcher;

public class QueryParser {

    //the dataset schema
    private DatasetSchema schema;

    //query structure
    private GroupBy groupBy;
    private Aggregator aggregator;
    private Pattern pattern;
    private ArrayList<Predicate> predicates;
    private Window window;



    public QueryParser(DatasetSchema schema){
        this.schema = schema;
    }

    /**
     * take in a few lines and parse them into a Query instance
     * @return a query instance
     */
    public Query parse(ArrayList<String> lines, Set<EventType> existedEventTypes){
        //parse order matters!
        //parse the patten first!
        parsePatternLine(lines.get(2), existedEventTypes);
        parseReturnLine(lines.get(1));
        parseWhereLine(lines.get(3));
        parseWindowLine(lines.get(5));
        return new Query(pattern, predicates, aggregator, groupBy, window);

    }

    private void parseReturnLine(String returnLine){
        String[] strings = returnLine.split(",");
        String returnString = strings[0];
        String aggString = strings[1];

        // the group by column
        /**
         * hardcoded here group event = kleene event
         */
        this.groupBy = new GroupBy(returnString.split("\\s")[1].split("\\.")[1],
                this.pattern.getEventTypes().get(this.pattern.getKleeneIndex()));

        //the aggregator
        String aggFunc = aggString.trim().split("\\(")[0];
        String aggColumn = aggFunc.equals("COUNT")? "" : aggString.trim().split("\\(")[1].split("\\)")[0].split("\\.")[1];
        this.aggregator = aggFunc.equals("COUNT")? new CountAggregator(aggFunc, aggColumn):
                new AvgAndSumAggregator(aggFunc, aggColumn);
    }

    private void parsePatternLine(String patternLine, Set<EventType> existedEventTypes){
        String line = patternLine.split("PATTERN ")[1];
        String patternString = line.startsWith("SEQ")? line.split("SEQ\\(")[1].split("\\)")[0] :line;
        this.pattern = new Pattern(patternString, this.schema, existedEventTypes);
    }

    private void parseWhereLine(String whereLine){
        String predicateString = whereLine.split("WHERE ")[1];
        String[] predicateStrings = predicateString.split("AND");
        this.predicates = new ArrayList<>();
        for (String pstr: predicateStrings){
            this.predicates.add(parseSingleWhere(pstr));
        }

    }

    private Predicate parseSingleWhere(String singleWhereString){
        ArrayList<EventType> predEventTypes = new ArrayList<>();

        predEventTypes.add(this.pattern.getEventTypes().get(this.pattern.getKleeneIndex()));
        String operator = "";
        ArrayList<String> attributes  = new ArrayList<>();

        String pa = "(.*?)([>=<])(.*?$)";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pa);
        Matcher a = p.matcher(singleWhereString);

        if (a.find()){
            String attr1 = a.group(1).trim().split("\\.")[1];
            attributes.add(attr1);

            operator = a.group(2);

            String[] right = a.group(3).trim().split("\\.");

            if (right.length==1){
                //single value
                Float value = Float.parseFloat(right[0]);
                return new SingleValuePredicate(predEventTypes, operator, attributes, schema, value);

            }else {//attribute comparison
                String attr2 = a.group(3).trim().split("\\.")[1];

                if (!attr1.equals(attr2)){
                    attributes.add(attr2);
                }
                return new AttributeComparisonPredicate(predEventTypes, operator, attributes, schema);

            }
        }
        System.out.printf("No valid predicates detected!");
        return null;
    }

    private void parseWindowLine(String windowLine){
        String pa1 = "(.*?)(\\d+)(.*)(\\d+)(.*?$)";
        java.util.regex.Pattern p1 = java.util.regex.Pattern.compile(pa1);
        Matcher a1 = p1.matcher(windowLine);
        if (a1.find()){
            long window = Integer.parseInt(a1.group(2));  //minutes
            long slide = Integer.parseInt(a1.group(4));   //minutes
            this.window = new Window(window, slide);
        }
    }
}
