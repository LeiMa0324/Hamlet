package hamlet.executor;

public class ExpMain {

    public static void main(String[] args){
        Experiment epw_exp = new Experiment(true);
        epw_exp.varyQueryNum();
        epw_exp.varyEpw();
    }
}
