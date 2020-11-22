package hamlet.base;

import hamlet.query.window.Window;

import java.math.BigInteger;
import java.util.ArrayList;

public class Pane {
    private long gcdWindow;
    public Pane(ArrayList<Window> windows){

        BigInteger gcd = BigInteger.ONE;
        for (Window w: windows){
            BigInteger windowDuration = new BigInteger(w.getWindowInMinute()+"");
            BigInteger slideDuration = new BigInteger(w.getSlidingMinute()+"");
            gcd = windowDuration.gcd(gcd).gcd(slideDuration);
        }

        this.gcdWindow = gcd.longValue();
    }
}
