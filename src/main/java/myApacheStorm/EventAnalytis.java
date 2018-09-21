package myApacheStorm;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import joinery.DataFrame;
import org.apache.commons.text.similarity.LevenshteinDistance;

import javax.xml.crypto.Data;


public class EventAnalytis {
    public static void main( String[] args )
    {
        final String savePath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_java/";
        String dataPath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/userVisits-Melb-tweets.csv";
        String POIpath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/POI-Melb.csv";
//        runProcess(savePath);
        try{
        DataFrame<Object> a = DataFrame.readCsv(dataPath);
            a.show();}
        catch (Exception e){
            e.printStackTrace();
        }

    }
    public static  void runProcess(String savePath){
        System.out.println("Start analysing tweets");
        try{
            DataFrame<Object> dfEventTweets = DataFrame.readCsv(savePath+"0915eventTweet.csv");
            DataFrame<Object> dfDetectedEventsResults = DataFrame.readCsv(savePath+"0915dfDetectedEventsResults.csv");
            DataFrame<Object> dfExpectedOutcome = DataFrame.readCsv(savePath+"expected.csv");
            dfDetectedEventsResults = dfDetectedEventsResults.join(dfExpectedOutcome,DataFrame.JoinType.OUTER);

            dfDetectedEventsResults.add("Step1_Only");
            dfDetectedEventsResults.add("Step2_Only");
            dfDetectedEventsResults.add("Step1_And_Step2");
            dfDetectedEventsResults.add("Step3_Only");
            dfDetectedEventsResults.add("Step1_And_Step3");
            dfDetectedEventsResults.add("Step2_And_Step3");
            dfDetectedEventsResults.add("Full_steps");
            int fullStepCol = dfDetectedEventsResults.size()-1;
            int step2n3Col = dfDetectedEventsResults.size()-2;
            int step1n3Col = dfDetectedEventsResults.size()-3;
            int step3Col = dfDetectedEventsResults.size()-4;
            int step1n2Col = dfDetectedEventsResults.size()-5;
            int step2Col = dfDetectedEventsResults.size()-6;
            int step1Col = dfDetectedEventsResults.size()-7;

            dfEventTweets=processText(dfEventTweets);

            int numOfEvents = dfEventTweets.unique("eventID").length();


            int numOfTweets = dfEventTweets.unique("tweetID").length();
            DataFrame<Object> dfProcessTweets = new DataFrame<Object>("eventID","poiID","tweetID","text","hashtags","processedtext");
            for (int i=1;i<=numOfEvents;i++){
                Long currentEventID = new Long(i);
                for (int j=0;j<numOfTweets;j++){
                    if (currentEventID.equals(dfEventTweets.col("eventID").get(j))){
                        dfProcessTweets.append(dfEventTweets.row(j));
                    }
                }
//                if(i==2){

                //Step1 only: distinct the text
                String result1 = step1Distinct(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step1Col,result1);
                //Step2 only: hashtags comparsion
                String result2 = step2Hashtags(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step2Col,result2);
                //Step 1n2: distinct and hashtags
                String result1n2= step1AndStep2(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step1n2Col,result1n2);


                //Full step
                String result = processEvent(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,fullStepCol,result);
//                }


                dfProcessTweets = new DataFrame<Object>("eventID","poiID","tweetID","text","hashtags","processedtext");
            }
            dfDetectedEventsResults.writeCsv(savePath+"analysis.csv");
            dfDetectedEventsResults.show();
        }
        catch (Exception e){
            e.printStackTrace();
        }



    }
    public static DataFrame<Object> processText(DataFrame<Object> dfEventTweets){
        //extract hashtags from text
        //delete common location hashtags "#Australia" and "#melbourne"
        //delete location info "@ somewhere"
        //delete short url "https://t.co/xxxxx"
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
    public static boolean processHashtags(DataFrame<Object> dfDistinctText){
        //for all tweets, 1st tweets'hashtags count+ with the rest hashtags
        //if any count>2/3 of total tweet label it as good event
        //Levenshtein distance is used to calculate the distance between two hashtags
        //similarity = (1/distance)/longerlength
        LevenshteinDistance distance = new LevenshteinDistance();
        int lengthOfTweets = dfDistinctText.length();
        for(int i =0;i<lengthOfTweets;i++){//the tweet to be counted
            ArrayList<String> currentHashtagsList = (ArrayList)dfDistinctText.col("hashtags").get(i);//store hashtags into list1
//            System.out.println("currentTweet:"+i+"\ncurrentList:"+currentHashtagsList);
            if(currentHashtagsList!=null){
                ArrayList<Integer> Counter = new ArrayList<Integer>();
                for (String hashtag: currentHashtagsList) {
                    Counter.add(1);//set initial state of Counter into 1
                }
                int lengthOfCurrentHashtags = currentHashtagsList.size();
                for(int j=i+1;j<lengthOfTweets;j++){//the rest tweets
                    ArrayList<String> otherHashtagsList = (ArrayList)dfDistinctText.col("hashtags").get(j);//store hashtages into list2
//                    System.out.println("otherTweet:"+j+"\notherList:"+otherHashtagsList);
                    if(otherHashtagsList!=null){
                        for(int index = 0;index<lengthOfCurrentHashtags;index++){
                            String input1 = currentHashtagsList.get(index);
                            Integer input1Length = input1.length();
                            for (String otherHashtag:otherHashtagsList) {
                                String input2 = otherHashtag;
                                Integer input2Length = input2.length();
                                Integer distanceInt = distance.apply(input1,input2);
                                Integer length = input1Length>=input2Length?input1Length:input2Length;
                                Integer similarityInt = length-distanceInt;
                                Double similarity = similarityInt.doubleValue()/length.doubleValue();
                                if(similarity>0.5){//if two hash tags has high similarity then consider they are similar hashtag
                                    Counter.set(index,Counter.get(index)+1);
                                    if(Counter.get(index)>lengthOfTweets*0.6){//if 60% of the tweets has similary hashtags then it is GoodEvent
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
    public static boolean processTextWithHashtags(DataFrame<Object> dfDistinctText){
        return false;
    }

    public static String step1Distinct(DataFrame<Object> dfProcessTweets){
        DataFrame<Object> dfDistinctText = dfProcessTweets.unique("processedtext");
        if(dfDistinctText.length()<4){
            return "NotEvent";
        }
        return "MaybeEvent";
    }

    public static String step2Hashtags(DataFrame<Object> dfProcessTweets){
        if(processHashtags(dfProcessTweets)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }

    public static String step1AndStep2(DataFrame<Object> dfProcessTweets){
        DataFrame<Object> dfDistinctText = dfProcessTweets.unique("processedtext");
        if(dfDistinctText.length()<4){
            return "NotEvent";
        }
        if(processHashtags(dfDistinctText)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }

    public static String processEvent(DataFrame<Object> dfProcessTweets){
//        for tweets in event:
//            text.distinct;
//        if count<4
//            label as NotEvent
//        if processHashtags() or processTextWithHashtags
//                label as GoodEvent
        DataFrame<Object> dfDistinctText = dfProcessTweets.unique("processedtext");
        if(dfDistinctText.length()<4){
            return "NotEvent";
        }
        if(processHashtags(dfDistinctText) || processTextWithHashtags(dfDistinctText)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }
}