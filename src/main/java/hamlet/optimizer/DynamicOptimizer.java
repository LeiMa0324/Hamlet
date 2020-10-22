package hamlet.optimizer;

import java.util.HashMap;

public class DynamicOptimizer {

    public DynamicOptimizer(){ }


    public boolean isToShare(HashMap<String, Integer> params){
        int sc = params.get("sc");
        int sp = params.get("sp");
        int k = params.get("k");
        int sharedg = params.get("sharedg'");
        int splitg = params.get("splitg");
        int p = params.get("p");
        int b = params.get("b");
        int n = params.get("n");



        Double sharedCost = sc*k*sharedg*p+b*(Math.log(sharedg)/Math.log(2)+n*sp);
        Double nonsharedCost = k*b*(Math.log(splitg)/Math.log(2)+n);
        Double benefit = nonsharedCost - sharedCost;

        System.out.println((benefit>0)?"============= choose to share============\n":"============= choose to split============\n");
        System.out.println("sc(number of snapshots created in a Burst): " + sc+"\n"+
        "sp(number of snapshots propagated): "+sp+
        "k(number of queries): "+k+"\n"+
        "shared g(number of events per shared graphlet):"+sharedg +"\n"+
        "non shared g(number of events per split graphlet):"+splitg +"\n"+
        "p(number of pred graphlet): "+p+"\n"+
        "b(Burst size): "+b+"\n"+
        "n(# of events): "+n+"\n"

        );
        System.out.println("shared cost :" +sharedCost);
        System.out.println("non shared cost :" +nonsharedCost);

        return benefit>0;
    }


}
