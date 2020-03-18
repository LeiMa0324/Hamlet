package DataGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Control the size of the stream and the number of shared events in one graphlet
 *
 * all unshared events has the same number unsharedNum in one graphlet
 * shared events has the number sharedNum in one graphlet
 */
public class StreamTemplate {
    private int length;
    private int unsharedNum;
    private int sharedNum;
    private String streamFile;

    public StreamTemplate(int length, int unsharedNum, int sharedNum, String streamFile){
        this.length = length;
        this.unsharedNum = unsharedNum;
        this.sharedNum = sharedNum;
        this.streamFile = streamFile;
    }

    public void generate(){
        try {
            File output_file = new File(streamFile);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int second_id = 0;

            OUT:
            for (int j =0;second_id< length;j++){
                //Generate 14 unshared event types, each event type has numofunshared consecutive events
                for (int e = 1;e<15;e++){
                    if (e==2){
                        continue;
                    }
                    for (int i=0;i<unsharedNum;i++){
                        output.append(String.valueOf(second_id)).append(",").append(String.valueOf(e)).append(",1").append('\n');
                        second_id++;
                        if (second_id==length){
                            break OUT;
                        }
                    }
                }

                //Generate shared events
                for (int i = 0; i< sharedNum; i++){
                    output.append(String.valueOf(second_id)).append(",").append("2").append(",1").append('\n');
                    second_id++;
                    if (second_id==length){
                        break OUT;
                    }
                }
            }
            output.close();
        } catch (IOException e) { e.printStackTrace(); }
    }
}
