package hamlet.query.aggregator;

import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * a class of the intermediate value
 *
 *
 */
@Data
public class Value {
    /**
     * count aggregator: one BigInteger value for count
     * AVG or Sum aggregator: BigDecimal for sum, BigInteger for count
     */
    private BigInteger count = BigInteger.ZERO;
    private BigDecimal sum = BigDecimal.ZERO;

    public static Value ZERO = new Value();
    public static Value ONE = new Value(BigInteger.ONE);


    public Value(){
    }
    public Value(BigInteger count){
        this.count = count;
    }

    public Value(BigInteger count, BigDecimal sum){
        this.count = count;
        this.sum = sum;
    }


    // add count and sum
    public Value add(Value value){

        Value newValue = new Value();
        newValue.setCount(this.count.add(value.getCount()));
        newValue.setSum(this.sum.add(value.getSum()));

        return newValue;
    }

    public Value substract(Value value){
        Value newValue = new Value();
        newValue.setCount(this.count.subtract(value.getCount()));
        newValue.setSum(this.sum.subtract(value.getSum()));

        return newValue;
    }

    //evaluate an expression of snapshots
    public Value multiply(BigInteger multiplier){
        Value newValue = new Value();

        //count = count* coeff
        newValue.setCount(this.count.multiply(multiplier));
        // sum = sum*coff + event.attr*count
        newValue.setSum(this.sum.multiply(new BigDecimal(multiplier)));

        return newValue;
    }

    public Value multiplyCount(BigInteger multiplier){
        Value newValue = new Value();
        newValue.setSum(this.sum);

        newValue.setCount(this.count.multiply(multiplier));
        return newValue;
    }

    public Value multiplySum(BigInteger multiplier){
        Value newValue = new Value();
        newValue.setCount(this.count);

        newValue.setSum(this.sum.multiply(new BigDecimal(multiplier)));
        return newValue;
    }

    public void incrementSum(BigDecimal added){
        this.setSum(this.sum.add(added));
    }


    public BigDecimal avg(){
        return this.count.equals(BigInteger.ZERO)?
                BigDecimal.ZERO:
                this.getSum().divide(new BigDecimal(this.count),2,BigDecimal.ROUND_HALF_UP);
    }

}
