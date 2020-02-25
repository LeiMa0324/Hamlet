package StreamGenerator;

import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class generator {
    public static void main(String[] agrs){
        for (int i =100; i<1100;i+=100){
            singleGenerator(i);
        }

    }
    static void singleGenerator(int numofSnapshots){
        String file_of_stream = String.format("src/main/resources/Streams/GenStream_%d.txt",numofSnapshots);
        try {
            File output_file = new File(file_of_stream);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int second_id = 0;
            for (int j =0;j<numofSnapshots;j++){
                //每一个snapshot random 3-5个A，2-4个C，10-20个B+
                Random random = new Random();
                int numofA = random.nextInt(3)+3;   //3-5个A
                int numofC = random.nextInt(3)+2;   //2-4个C
                int numofB = random.nextInt(11)+10; //10-20个B

                for (int i=0;i<numofA;i++){
                    output.append(String.valueOf(second_id)).append(",").append("1").append(",1").append('\n');
                    second_id++;
                }
                for (int i=0;i<numofC;i++){
                    output.append(String.valueOf(second_id)).append(",").append("3").append(",1").append('\n');
                    second_id++;
                }
                for (int i=0;i<numofB;i++){
                    output.append(String.valueOf(second_id)).append(",").append("2").append(",1").append('\n');
                    second_id++;
                }

            }
            output.close();
        } catch (IOException e) { e.printStackTrace(); }


    }
}
