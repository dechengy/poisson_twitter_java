package myApacheStorm;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import joinery.DataFrame;
import org.apache.commons.text.similarity.LevenshteinDistance;


public class EventAnalytis {
    public static void main( String[] args )
    {
        final String savePath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_java/";
        String dataPath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/userVisits-Melb-tweets.csv";
        String POIpath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/POI-Melb.csv";
        runProcess(savePath);
        outcomeAnalysis(savePath);
//        try{
//        DataFrame<Object> a = DataFrame.readCsv(dataPath);
//            a.show();}
//        catch (Exception e){
//            e.printStackTrace();
//        }

    }
    public static void outcomeAnalysis(String savePath){
        try{
            DataFrame<Object> dfDetectedEventsResults = DataFrame.readCsv(savePath+"analysis.csv");
            int count =0;
            for (int i =0;i<dfDetectedEventsResults.length();i++){
                if(dfDetectedEventsResults.col("Expected Outcome").get(i).toString().equals(
                        dfDetectedEventsResults.col("Full_steps").get(i).toString()
                )){
                    count++;
                    System.out.println("Event:"+dfDetectedEventsResults.col("eventID").get(i) +
                            " detected as: "+ dfDetectedEventsResults.col("Full_steps").get(i));
                }else
                {
                    System.out.println("Event:"+dfDetectedEventsResults.col("eventID").get(i) +
                            " should be: "+ dfDetectedEventsResults.col("Expected Outcome").get(i) +
                            " but get: "+ dfDetectedEventsResults.col("Full_steps").get(i));

                }
            }
            System.out.println("Accuracy: "+((double)count)/dfDetectedEventsResults.length());
//            dfDetectedEventsResults.show();


        }
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
//            dfEventTweets.show();
            int numOfEvents = dfEventTweets.unique("eventID").length();


            int numOfTweets = dfEventTweets.unique("tweetID").length();
            DataFrame<Object> dfProcessTweets = new DataFrame<Object>("eventID","poiID","userID","tweetID","text","hashtags","processedtext");
            for (int i=1;i<=numOfEvents;i++){
                Long currentEventID = new Long(i);
                for (int j=0;j<numOfTweets;j++){
                    if (currentEventID.equals(dfEventTweets.col("eventID").get(j))){
                        dfProcessTweets.append(dfEventTweets.row(j));
                    }
                }
//                if(i==11){

                //Step1 only: distinct the text and author
                String result1 = step1Distinct(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step1Col,result1);
                //Step2 only: hashtags comparsion
                String result2 = step2Hashtags(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step2Col,result2);
                //Step 1n2: distinct and hashtags
                String result1n2= step1AndStep2(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step1n2Col,result1n2);
                //Step3 only: process text with hashtags
                String result3 = step3Texts(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step3Col,result3);
                //Step 1n3: distinct and text with hashtags
                String result1n3 = step1And3(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step1n3Col,result1n3);
                //Step 2n3: hashtags and text with hashtags
                String result2n3 =step2And3(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,step2n3Col,result2n3);

                //Full step:
                String result = processEvent(dfProcessTweets);
                dfDetectedEventsResults.set(i-1,fullStepCol,result);
//                }


                dfProcessTweets = new DataFrame<Object>("eventID","poiID","tweetID","userID","text","hashtags","processedtext");
            }
            dfDetectedEventsResults.show();
            dfDetectedEventsResults.writeCsv(savePath+"analysis.csv");
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
                dfEventTweets.set(i,5,hashtags);
            }
            text=text.replaceAll("https://t.co/[a-zA-Z0-9]+","")
                    .replaceAll("\\@ [a-zA-Z0-9\\s,-]+","")
                    .replaceAll("#[a-zA-Z0-9]+\\s","")
                    .toLowerCase();
            dfEventTweets.set(i,6,text);//remove location info, hashtags and url
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
                                    if(Counter.get(index)>=(int)(lengthOfTweets*0.6)){//if 60% of the tweets has similary hashtags then it is GoodEvent
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
        //if the hashtags does not recognize the event, we can consider the text along with hash tags
        //here we consider the phases of text as hashtags then compare with other hashtags
        LevenshteinDistance distance = new LevenshteinDistance();
        int lengthOfTweets = dfDistinctText.length();
        for(int i =0;i<lengthOfTweets;i++) {//the tweet to be counted
            ArrayList<String> currentHashtagsList = (ArrayList) dfDistinctText.col("hashtags").get(i);//store hashtags into list1
            if (currentHashtagsList == null) {
                currentHashtagsList = new ArrayList<String>();
                String text = (String) dfDistinctText.col("text").get(i);
                if (text.length() > 0) {
                    //if tweet has text then split it, add into hashtags
                    String[] textSplited = text.split(" ");
                    if (textSplited.length > 0) {
                        for (String phase : textSplited) {
                            currentHashtagsList.add(phase);
                        }
                    }
                }
            }
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
                                    if(Counter.get(index)>=(int)(lengthOfTweets*0.4)){//if 60% of the tweets has similary hashtags then it is GoodEvent
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

    public static String step1Distinct(DataFrame<Object> dfProcessTweets){
        DataFrame<Object> dfDistinctText = dfProcessTweets.unique("processedtext");
        DataFrame<Object> dfDistinctUser = dfProcessTweets.unique("userID");
        if(dfDistinctText.length()<4 || dfDistinctUser.length()<3){
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
        DataFrame<Object> dfDistinctUser = dfProcessTweets.unique("userID");
        if(dfDistinctText.length()<4 || dfDistinctUser.length()<3){
            return "NotEvent";
        }
        if(processHashtags(dfDistinctText)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }

    public static String step3Texts(DataFrame<Object> dfProcessTweets){
        if(processTextWithHashtags(dfProcessTweets)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }

    public static String step1And3(DataFrame<Object> dfProcessTweets){
        DataFrame<Object> dfDistinctText = dfProcessTweets.unique("processedtext");
        DataFrame<Object> dfDistinctUser = dfProcessTweets.unique("userID");
        if(dfDistinctText.length()<4 || dfDistinctUser.length()<3){
            return "NotEvent";
        }
        if(processTextWithHashtags(dfDistinctText)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }

    public static String step2And3(DataFrame<Object> dfProcessTweets){
        if(processHashtags(dfProcessTweets)){
            return "GoodEvent";
        }
        else if(processTextWithHashtags(dfProcessTweets)){
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
        DataFrame<Object> dfDistinctUser = dfProcessTweets.unique("userID");
        if(dfDistinctText.length()<5 || dfDistinctUser.length()<5){
            return "NotEvent";
        }
        if(processHashtags(dfDistinctText)){
            return "GoodEvent";
        }else if(processTextWithHashtags(dfDistinctText)){
            return "GoodEvent";
        }
        return "MaybeEvent";
    }
}
