package test;

import com.sun.swing.internal.plaf.synth.resources.synth_sv;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/29/14.
 */
public class Tester
{
    public static void main(String[] args) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("/home/danbox/Documents/cs455/cs455_asg3/src/test/test.txt"));

        String line = br.readLine();
        StringBuilder stringBuilder = new StringBuilder();
        while(line != null)
        {
            String[] split = line.split("\\s+");
            if(split.length == 3)
            System.out.println(split[0] + " " + split[1] + " " + split[2]);
//            stringBuilder.append(line);
//            stringBuilder.append(('\n'));
            line = br.readLine();
//            System.out.print(".");
        }
//        StringTokenizer tok = new StringTokenizer(stringBuilder.toString(), "\\s+");
//        String[] all = stringBuilder.toString().split("[\\s\\n\\t]+");
//        FileWriter fw = new FileWriter(new File("/home/danbox/Documents/cs455/cs455_asg3/src/test/temp.txt"));
//        BufferedWriter bw = new BufferedWriter(fw);
//        for(int i = 0; i < all.length; ++ i)
//        {
//            bw.write(all[i] + i + '\n');
//        }
//     bw.close();
    }
}
