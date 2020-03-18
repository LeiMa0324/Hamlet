package DataGenerator;

import lombok.Data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * control the number of queries, the length of queries, the position of shared events
 * all queries in the workload share the same setting
 */
@Data
public class WorkloadTemplate {
    private String query;
    private String queryFile;
    private int num;
    private int length;
    private int sharedPos;

    public WorkloadTemplate(int num,int length, int sharedPos, String queryFile){
        this.query = "";
        this.num = num;
        this.queryFile = queryFile;
        this.length = length;
        this.sharedPos = sharedPos; //start from 1

    }

    public void generate(){
        try{
            File output_file = new File(this.queryFile);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));

            for (int i=0;i<this.num;i++){

                int lastEvent = 0;

                for (int l =0;l<length;l++){
                    if (l ==sharedPos){
                        output.append(",2+");
                        lastEvent =2;
                        continue;
                    }

                    Random random = new Random();
                    int randomUnshared = random.nextInt(15);
                    while(randomUnshared == lastEvent||randomUnshared == 2){
                        randomUnshared = random.nextInt(15);
                    }
                    lastEvent = randomUnshared;
                    String tmp = l==0?randomUnshared+"":","+randomUnshared;
                    output.append(tmp);
                    if (l ==length-1){
                        output.append("\n");
                    }
                }
            }
            output.close();
        }catch (IOException e) { e.printStackTrace(); }
    }

}
