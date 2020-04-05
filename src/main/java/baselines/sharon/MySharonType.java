package baselines.sharon;

import lombok.Data;

import java.math.BigInteger;

@Data
public class MySharonType {
    public boolean isSTART;
    public boolean isTRIG;
    public boolean isKleene;    //is the event type has +
    public boolean notCalculated;
    public Integer repeats;     // the repeat times
    public Integer calculatedRepeats;     // the calculated repeat times

    public MySharonType predecessor;
    public int current_second;	//当前时间
    public BigInteger current_second_count;		//当前计数
    public BigInteger previous_second_count;	//上一次的计数

    /**
     * for start event
     */
    public MySharonType() {
        isSTART = true;
        isTRIG = false;
        isKleene = false;
        repeats = 1;
        calculatedRepeats = 0;
        resetTime();
        notCalculated = true;
    }

    /**
     * for other event
     * @param isTRIG

     */
    public MySharonType(boolean isTRIG) {
        this.isSTART = false;
        this.isTRIG = isTRIG;
        this.isKleene = false;
        this.repeats = 1;
        calculatedRepeats = 0;
        notCalculated = true;

        resetTime();
    }

    public void updateTime(int t) {
        current_second = t;
        previous_second_count = current_second_count;	//将当前计数给上一次计数
    }

    public void resetTime() {
        current_second = -1;
        current_second_count = new BigInteger("0");
        previous_second_count = new BigInteger("0");
    }
}
