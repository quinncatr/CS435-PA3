package tfidf;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.BytesWritable;

public class Book 
{
    public static class Doc 
    {
        public final String id;
        public final String title;
        public final String body;

        public Doc(String id, String title, String body) 
        {
            this.id = id;
            this.title = title;
            this.body = body;
        }
    }

    private static final Pattern HEADER_SPLIT =
        Pattern.compile("(?m)^([^\\n<]+)\\s*<====>\\s*(\\d+)\\s*<====>\\s*", Pattern.MULTILINE);

    public static List<Doc> parseAll(BytesWritable bytes) 
    {
        String all = new String(bytes.copyBytes(), StandardCharsets.UTF_8);
        Matcher m = HEADER_SPLIT.matcher(all);
        List<Doc> out = new ArrayList<>();
        int start = -1;
        String title = null;
        String id = null;

        while(m.find()) 
        {
            if(id != null && start >= 0) 
            {
                String prevBody = all.substring(start, m.start());
                out.add(new Doc(id, title, prevBody));
            }

            title = m.group(1).trim();
            id = m.group(2).trim();
            start = m.end();
        }

        if(id != null && start >= 0) 
        {
            String body = all.substring(start);
            out.add(new Doc(id, title, body));
        }

        return out;
    }

    public static boolean keepWord(String tok) 
    {
        if(tok == null) 
        {
            return false;
        }

        if(tok.isEmpty()) 
        {
            return false;
        }

        if(!tok.matches(".*[a-z].*")) 
        {
            return false;
        }

        if(tok.matches("\\d+(?:-\\d+)*")) 
        {
            return false;
        }

        if(tok.length() > 40) 
        {
            return false;
        }

        if(tok.matches("[acgt]{20,}")) 
        {
            return false;
        }

        return true;
    }

    public static String normalizeUnigrams(String s) 
    {
        String x = s.toLowerCase();
        x = x.replace("'", "");
        x = x.replaceAll("-{2,}", " ");                    
        x = x.replaceAll("-(?![a-z])|(?<![a-z])-", " ");   
        x = x.replaceAll("[^a-z0-9\\-\\s]", " ");          
        x = x.replaceAll("\\s+", " ").trim();
        return x;
    }

    public static java.util.List<String> getUnigram(String body)
    {
        String norm = normalizeUnigrams(body);
        List<String> unigramLst = new ArrayList<>();

        if(!norm.isEmpty()) 
        {
            String[] splt = norm.split("\\s+");

            for(String t : splt) 
            {
                boolean keep = keepWord(t);

                if(keep) 
                {
                    unigramLst.add(t);
                }
            }
        }

        return unigramLst;
    }

}
