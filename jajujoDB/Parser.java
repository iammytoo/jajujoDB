package jajujoDB;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
    public static final Pattern PROTECT_PATTERN = Pattern.compile("\\([^()]*\\)");
    public static final Pattern B_PATTERN = Pattern.compile("\\(([^)]*)\\)");
    public static void main(String[] args) {
        String input = "createtable users 3 (hoge,int) (fuga,string)";
        for(String a : splitString(input)){
            System.out.println(a);
            
            if(bracketSplit(a) != null) for(String x : bracketSplit(a))System.out.println(x);
        }

    }

    public static String[] splitString(String input) {
        Matcher protectMatcher = PROTECT_PATTERN.matcher(input);
        
        ArrayList<String> protectedTexts = new ArrayList<>();
        int i = 0;
        while (protectMatcher.find()) {
            protectedTexts.add(protectMatcher.group());
            input = input.replace(protectMatcher.group(), "PROTECTED" + i);
            i++;
        }
        
        String[] words = input.split("\\s+");

        for (int j = 0; j < words.length; j++) {
            if (words[j].startsWith("PROTECTED")) {
                int index = Integer.parseInt(words[j].substring(9));
                words[j] = protectedTexts.get(index);
            }
        }

        return words;
    }

    public static String[] bracketSplit(String input) {
        Matcher matcher = B_PATTERN.matcher(input);

        if (matcher.find()) {
            String insideParentheses = matcher.group(1).trim();

            String[] elements = insideParentheses.split("\\s*,\\s*");
            return elements;
        }
        return null;
    }
}
