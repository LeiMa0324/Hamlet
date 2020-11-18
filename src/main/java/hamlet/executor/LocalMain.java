package hamlet.executor;

public class LocalMain {

    public static void main(String[] args){
        Experiment epw_exp = new Experiment(true);
        epw_exp.varyQueryNum();
        epw_exp.varyEpw();
    }
}
