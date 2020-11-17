package hamlet.executor;

public class serverMain {

    public static void main(String[] args){
        Experiment experiment = new Experiment(false);
        experiment.varyQueryNum();
        experiment.varyEpw();
    }

}
