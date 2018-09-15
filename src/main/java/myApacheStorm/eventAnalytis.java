package myApacheStorm;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import joinery.DataFrame;

import javax.xml.crypto.Data;

public class eventAnalytis {
    public static void main( String[] args )
    {
        final String savePath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_java/";
        System.out.println("Start analysing tweets");
        try{
        DataFrame<Object> dfEventTweets = DataFrame.readCsv(savePath+"0915eventTweet.csv");

        dfEventTweets=processText(dfEventTweets);

        dfEventTweets.show();

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public static DataFrame<Object> processText(DataFrame<Object> dfEventTweets){
        dfEventTweets.add("hashtags").add("processedtext");


        for(int i=0;i<dfEventTweets.length();i++){
            String text = (String)dfEventTweets.col("text").get(i);
            Pattern pattern = Pattern.compile("#(\\w+)");
            Matcher matcher = pattern.matcher(text);
            ArrayList<String> hashtags = new ArrayList<String>();
            while (matcher.find()) {
                String matched = matcher.group(1);
                if(!matched.equals("Australia") && !matched.equals("Melbourne")){
                    hashtags.add(matcher.group(1).toLowerCase());
                }
            }
            if (hashtags.size()>0){
                dfEventTweets.set(i,4,hashtags);
            }
            text=text.replaceAll("https://t.co/[a-zA-Z0-9]+","")
                    .replaceAll("\\@ [a-zA-Z0-9\\s,-]+","")
                    .replaceAll("#[a-zA-Z0-9]+\\s","")
                    .toLowerCase();
            dfEventTweets.set(i,5,text);//remove location info, hashtags and url
        }

        return dfEventTweets;
    }
    public static boolean processHashtags(){

        return true;
    }
}
