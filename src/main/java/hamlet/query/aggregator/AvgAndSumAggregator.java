package hamlet.query.aggregator;

import hamlet.base.Event;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * the aggregator of avg and sum
 * todo: split avg, sum
 */
public class AvgAndSumAggregator extends Aggregator {

    public AvgAndSumAggregator(String funcName, String attributeName) {
        super(funcName, attributeName);
    }

    /**
     * given a bunch of new event, update the sum and number
     * @param events a bunch of new event
     */
    public void aggregate(ArrayList<Event> events) {

        BigDecimal sum = this.values.isEmpty()? BigDecimal.ZERO : (BigDecimal) values.get(0);
        Integer number = 0;

        if (this.func == Aggregfunctions.AVG) {
            number = this.values.isEmpty() ? 0 : (Integer) values.get(1);
        }

        for (Event e : events) {
            sum = sum.add(new BigDecimal((Float) e.getAttributeValueByName(this.attributeName)));
            if (this.func == Aggregfunctions.AVG) {
                number = number + 1;

            }

        }

        values.set(0, sum);
        values.set(1, number);

    }

    @Override
    public String toString(){
        return this.func==Aggregfunctions.AVG?"AVG("+attributeName+")":"SUM("+attributeName+")";
    }
}
