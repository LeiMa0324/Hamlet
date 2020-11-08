package hamlet.users.stockUser;

import hamlet.executor.Experiment;

public class ExpMain {

    public static void main(String[] args){
        Experiment epw_exp = new Experiment();
//        epw_exp.varyQueryNum();
        epw_exp.varyEpw();
    }
}
