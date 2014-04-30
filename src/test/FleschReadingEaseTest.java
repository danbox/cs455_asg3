package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class FleschReadingEaseTest
{
    public static void main(String[] args) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("/home/danbox/Documents/cs455/cs455_asg3/src/test/test.txt"));

        String line = br.readLine();
        int wordCount = 0;
        int syllableCount = 0;
        int sentenceCount = 0;
        while(line != null)
        {
            List<String> split = new ArrayList<String>();
            split.addAll(Arrays.asList(line.split("\\s+")));
            for(String curr : split)
            {
                if(curr.length() != 0)
                {
                    wordCount += 1;
                    syllableCount += countSyllables(curr);

                    char lastChar = curr.charAt(curr.length() - 1);

                    if(lastChar == '.' || lastChar == '!' || lastChar == '?')
                    {
                        sentenceCount += 1;
                    }
                }
            }
            line = br.readLine();
//            System.out.print(".");
        }
        double readingEase = 206.835 - 1.015 * (wordCount / sentenceCount) - 84.6 * (syllableCount / wordCount);
        System.out.println(readingEase);
        double gradeLevel = .39 * (wordCount / sentenceCount) + 11.8 * (syllableCount / wordCount) - 15.59;
        System.out.println(gradeLevel);
    }

    private static int countSyllables(String word)
    {
        char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
        String currentWord = word;
        int numVowels = 0;
        Boolean lastWasVowel = false;
        for(char wc : currentWord.toCharArray())
        {
            Boolean foundVowel = false;
            for(char v : vowels)
            {
                //don't count diphthongs
                if (v == wc && lastWasVowel)
                {
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                }
                else if (v == wc && !lastWasVowel)
                {
                    numVowels++;
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                }
            }

            //if full cycle and no vowel found, set lastWasVowel to false;
            if (!foundVowel)
                lastWasVowel = false;
        }
        //remove es, it's _usually? silent
        if (currentWord.length() > 2 && currentWord.substring(currentWord.length() - 2) == "es")
            numVowels--;
            // remove silent e
        else if (currentWord.length() > 1 && currentWord.substring(currentWord.length() - 1) == "e")
            numVowels--;

        return numVowels;
    }
}
