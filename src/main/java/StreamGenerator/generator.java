package StreamGenerator;

import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

//Todo 完成Generator
//Todo run several preliminary tests to figure out a good range for the number of b's we evaluate.

/**
 * SEQ pattern: A, E, C, F, B
 * every loop has the same number of A, E, C, F, we vary the number of Bs
 */
public class generator {
    public static void main(String[] agrs){
        int numofevents = 1000000;      //fix the number of events in the stream file
        Random random = new Random();
        int numofA = random.nextInt(3)+3;   //fixed number of A  1
        int numofE = random.nextInt(3)+5;   //fixed number of E   4
        int numofC = random.nextInt(3)+2;   //fixed number of C   3
        int numofF = random.nextInt(3)+5;   //fixed number of F   5
        for (int numofB = 20; numofB<40;numofB++){      //vary number of Bs for each stream file
            singleGenerator(numofevents,numofB, numofA, numofE,numofC,numofF);
        }

    }

    static void singleGenerator(int numofevents, int numofB, int numofA, int numofE, int numofC, int numofF){
        String file_of_stream = String.format("src/main/resources/Streams/GenStream_%d.txt",numofB);
        try {
            File output_file = new File(file_of_stream);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int second_id = 0;
            for (int j =0;second_id< numofevents;j++){

                //Generate A
                for (int i=0;i<numofA;i++){
                    output.append(String.valueOf(second_id)).append(",").append("1").append(",1").append('\n');
                    second_id++;
                }
                //Generate E
                for (int i=0;i<numofE;i++){
                    output.append(String.valueOf(second_id)).append(",").append("4").append(",1").append('\n');
                    second_id++;
                }
                //Generate C
                for (int i=0;i<numofC;i++){
                    output.append(String.valueOf(second_id)).append(",").append("3").append(",1").append('\n');
                    second_id++;
                }
                //Generate F
                for (int i=0;i<numofF;i++){
                    output.append(String.valueOf(second_id)).append(",").append("5").append(",1").append('\n');
                    second_id++;
                }

                //Generate B
                for (int i=0;i<numofB;i++){
                    output.append(String.valueOf(second_id)).append(",").append("2").append(",1").append('\n');
                    second_id++;
                }

            }
            output.close();
        } catch (IOException e) { e.printStackTrace(); }

    }
}
