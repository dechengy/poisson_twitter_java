package myApacheStorm;

import joinery.DataFrame;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class App
{
    public static void main( String[] args )
    {
        //absolute path of POI information, tweet data and save path
        String POIpath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/POI-Melb.csv";
        String dataPath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/userVisits-Melb-tweets.csv";
        String savePath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_java/";
        //time block of detecting
        Integer timeBlock = 600;
        try{
            // In[1]
            System.out.println("\nIn[1]:");
            DataFrame<Object> dfMelbPOI = DataFrame.readCsv(POIpath,";");
            DataFrame<Object> dfTweets = new DataFrame<>();
            dfTweets = DataFrame.readCsv(dataPath,",");
            System.out.println(dfTweets.size());
//            dfTweets.head(5).show();
            // In[0]

//            poiID:java.lang.Long
//            theme:java.lang.String
//            subTheme:java.lang.String
//            poiName:java.lang.String
//            lat:java.lang.Double
//            long:java.lang.Double


//            poiID:java.lang.Long
//            tweetID:java.lang.Double
//            userID:java.lang.Long
//            created_at:java.lang.String
//            melbTime_created_at:java.lang.String
//            createdWeekday:java.lang.Long
//            createdHour:java.lang.Long
//            createdDay:java.lang.Long
//            createdMonth:java.lang.Long
//            createdYear:java.lang.Long
//            lat:java.lang.Double
//            long:java.lang.Double
//            text:java.lang.String
//            poiDist:java.lang.Double
//            poiLat:java.lang.Double
//            poiLong:java.lang.Double
//            poiTheme:java.lang.String
//            poiName:java.lang.String
//            poiFreq:java.lang.Long

            for(int i =0;i<dfTweets.size();i++){
                System.out.println(dfTweets.columns().toArray()[i]+ ":" +dfTweets.col(i).get(0).getClass().getName());

            }



            // In[2]
            //check if data is correct
            System.out.println("\nIn[2]:");
            System.out.println("Number of Tweets:"+dfTweets.unique("tweetID").length());
            System.out.println("Number of Users:"+dfTweets.unique("userID").length());

            List<Object> usersID = dfTweets.col("userID");
            List<Object> uniqueUsersID = new ArrayList<>(
                    new HashSet<>(usersID)
            );
            List<Object> tweetID = dfTweets.col("tweetID");
            List<Object> uniqueTweetID = new ArrayList<>(
                    new HashSet<>(tweetID)
            ); //getClass().getName(): java.lang.Double
            ArrayList<Double> uniqueTweetIDdouble = new ArrayList<Double>();
            for(int i =0;i<uniqueTweetID.size();i++){
                uniqueTweetIDdouble.add((Double)uniqueTweetID.get(i));
            }
            Double MaxTweetID = Collections.max(uniqueTweetIDdouble);
            Double MinTweetID = Collections.min(uniqueTweetIDdouble);

            for(int i =0;i<uniqueTweetID.size();i++){
                if (dfTweets.col("tweetID").get(i).equals(MaxTweetID)){
                    System.out.println("Max tweetID Tweet:\n" + dfTweets.row(i));
                }
                if (dfTweets.col("tweetID").get(i).equals(MinTweetID)){
                    System.out.println("Min tweetID Tweet:\n" + dfTweets.row(i));
                }
            }
            System.out.println("\n");

//            In[3]
            //
            System.out.println("\nIn[3]:");
            DataFrame<Object> dfTweets2017 = new DataFrame<Object>(dfTweets.columns());
            System.out.println(dfTweets2017.columns());
            for(int i =0;i<dfTweets.length();i++){
                if(dfTweets.col("createdYear").get(i).equals(new Long("2017")) && dfTweets.col("createdMonth").get(i).equals(new Long("1"))){
//                    for consistency, only consider tweets in 2017 (where the full year tweets are available)
//          createdYear.getClass().getName()=java.lang.Long
//          createdMonth.getClass().getName()=java.lang.Long
                    dfTweets2017.append(dfTweets.row(i));
                }
            }
//            In[4]
//            check unix timestamp
            System.out.println("\nIn[4]:");
            dfTweets2017.add("unixtimeMin");
            System.out.println("Length of dfTweets2017: "+dfTweets2017.length());
            System.out.println("\n");
//            melbTime_created_at.getClass().getName()=java.lang.String
//            Fri Jan 20 06:30:33 +1100 2017
//            EEE MMM dd hh:mm:ss +1100 yyyy
            long unixtimeMin=0;
            int unixtimeMinIndex = dfTweets2017.columns().size()-1;

            long minTime = 2000000000;
            long maxTime = 0;//For In[6]
            for(int i =0;i<dfTweets2017.length();i++){
                unixtimeMin = unixtimeMin(dfTweets2017.col("melbTime_created_at").get(i).toString());
                dfTweets2017.set(i,unixtimeMinIndex,unixtimeMin);
                if(i<10){
                    System.out.println(unixtimeMin);
                }
                if(unixtimeMin > maxTime){
                    maxTime = unixtimeMin;
                }
                if(unixtimeMin < minTime){
                    minTime = unixtimeMin;
                }
            }
//            In[5]
//            check test data
            System.out.println("\nIn[5]:");
//            lat/long.getClass().getName()=java.lang.Double
            List<Object> tweetLat = dfTweets.col("lat");
            ArrayList<Double> latDouble = new ArrayList<Double>();
            for(int i =0;i<tweetLat.size();i++){
                latDouble.add((Double)tweetLat.get(i));
            }

            List<Object> tweetLong = dfTweets.col("long");
            ArrayList<Double> longDouble = new ArrayList<Double>();
            for(int i =0;i<tweetLong.size();i++){
                longDouble.add((Double)tweetLong.get(i));
            }

            Double minLat = Collections.min(latDouble);
            Double maxLat = Collections.max(latDouble);
            Double lenLat = maxLat - minLat;
            Double midLat = minLat + (lenLat/2);

            Double minLong = Collections.min(longDouble);
            Double maxLong = Collections.max(longDouble);
            Double lenLong = maxLong - minLong;
            Double midLong = maxLong + (lenLong/2);

            System.out.println(dfTweets2017.col("text").get(0));
            System.out.println(dfTweets2017.col("text").get(1));

//            In[6]
//            process previous tweet data for 3 days
            System.out.println("\nIn[6]:");
            Integer pastWinSize = 3 * 24 * 60 * 60; //in seconds = 3 days
            //minTime and maxTime already definded in In[4]
            Long lastTime = minTime + pastWinSize;
            DataFrame<Object> dfLambdas = new DataFrame<>("poiID", "totalTweets", "firstTweetTime");
            for(int i=0;i<dfMelbPOI.length();i++){
                dfLambdas.append(Arrays.asList(dfMelbPOI.col("poiID").get(i),1,minTime));
                //set to default 1 to avoid lambda=0 from triggering everything
            }
            System.out.println("minTime: "+lastTime);//1483448400
            System.out.println("maxTime: "+maxTime);//1485865800

            for(int i =0;i<dfTweets2017.length();i++){
//                poiID.getClass().getName()=java.lang.Long
                if(Long.parseLong(dfTweets2017.col("unixtimeMin").get(i).toString())<lastTime){
                    String poiID = dfTweets2017.col("poiID").get(i).toString();
                    Integer index = Integer.parseInt(poiID)-1; //index of row in dfLambdas
                    Integer count = Integer.parseInt(dfLambdas.col(1).get(index).toString());
                    count=count+1;
                    dfLambdas.set(index,1,count);
                    if(Long.parseLong(dfLambdas.col(2).get(index).toString())>Long.parseLong(dfTweets2017.col("unixtimeMin").get(i).toString())){
                        dfLambdas.set(index,2,dfTweets2017.col("unixtimeMin").get(i));
                    }
                }
            }



//            In[7]
//            detecting events
            System.out.println("\nIn[7]:");
            boolean isDetectionSucceed = detectAndSaveEvents(dfTweets2017,dfLambdas,savePath,minTime,maxTime,lastTime,timeBlock);
            if(isDetectionSucceed){
                System.out.println("Succeed detecting events");
            }

            //In[8]
//            check results
            System.out.println("\nIn[8]");
            DataFrame<Object> dfDetectedEvents = DataFrame.readCsv(savePath+"0915dfDetectedEvents.csv");
            DataFrame<Object> dfEvents = dfDetectedEvents.add("eventID");
            Integer IDindex = dfEvents.size()-1;
            for(int i=0;i<dfEvents.length();i++){
                dfEvents.set(i,IDindex,i+1);

            }

            System.out.println(dfMelbPOI.columns());
            System.out.println(dfEvents.columns());
            saveEventsWithPOIinfo(dfEvents,dfMelbPOI,savePath);

            //In[9]
//            extract tweets of events out
            System.out.println("\nIn[9]");
            boolean isSaveTweetsOfEventSucceed = saveTweetsOfEvent(dfTweets2017,dfEvents,savePath);
            if(isSaveTweetsOfEventSucceed){
                System.out.println("Succeed saving tweets of events");
            }

            //In[10]
            System.out.println("\nIn[10]");
//            DataFrame<Object> dfEventTweets = DataFrame.readCsv(savePath+"0915eventTweet.csv");
//            dfEventTweets.show();

            //for the tweets of events, go class EventAnalytis.java

        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void saveEventsWithPOIinfo(DataFrame<Object> dfEvents,DataFrame<Object> dfMelbPOI,String savePath){
        DataFrame<Object> dfDetectedEventsResults = new DataFrame<Object>("poiID"
        ,"theme","subTheme","poiName","lat","long","algo","eventSignal","timeStarted","timeEnded","eventID");
        for(int i =0;i<dfEvents.length();i++){
            Long tempID2 = (Long)dfEvents.col("poiID").get(i);
                for(int j =0;j<dfMelbPOI.length();j++){
                    Long tempID1 = (Long)dfMelbPOI.col("poiID").get(j);
                    if(tempID1.equals(tempID2)){
                        dfDetectedEventsResults.append(Arrays.asList(
                            dfMelbPOI.col("poiID").get(j),
                                dfMelbPOI.col("theme").get(j),
                                dfMelbPOI.col("subTheme").get(j),
                                dfMelbPOI.col("poiName").get(j),
                                dfMelbPOI.col("lat").get(j),
                                dfMelbPOI.col("long").get(j),
                                dfEvents.col("algo").get(i),
                                dfEvents.col("eventSignal").get(i),
                                dfEvents.col("timeStarted").get(i),
                                dfEvents.col("timeEnded").get(i),
                                dfEvents.col("eventID").get(i)
                        ));
                    }
            }
        }
        try{
        dfDetectedEventsResults.writeCsv(savePath+"0915dfDetectedEventsResults.csv");}
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static boolean saveTweetsOfEvent(DataFrame<Object> dfTweets2017,DataFrame<Object> dfEvents,String savePath){
        DataFrame<Object> dfEventTweets = new DataFrame<Object>("eventID","poiID","userID","tweetID","text");
        for (int i =0;i<dfTweets2017.length();i++){
            Long dfTweets2017poiID = (Long)dfTweets2017.col("poiID").get(i);
            Long dfTweets2017unixtimeMin = (Long)dfTweets2017.col("unixtimeMin").get(i);
            for(int j =0;j<dfEvents.length();j++) {
                Long dfEventstimeStarted = (Long) dfEvents.col("timeStarted").get(j);
                Long dfEventstimeEnded = (Long) dfEvents.col("timeEnded").get(j);
                Long dfEventspoiID = (Long) dfEvents.col("poiID").get(j);
                if (dfTweets2017poiID.equals(dfEventspoiID) && dfTweets2017unixtimeMin >= dfEventstimeStarted
                        && dfTweets2017unixtimeMin < dfEventstimeEnded) {
                    dfEventTweets.append(
                            Arrays.asList(
                                    dfEvents.col("eventID").get(j),
                                    dfEventspoiID,
                                    dfTweets2017.col("userID").get(i),
                                    dfTweets2017.col("tweetID").get(i),
                                    dfTweets2017.col("text").get(i)
                            )
                    );
                }
            }
        }
        try{
        dfEventTweets.sortBy("eventID").writeCsv(savePath+"0915eventTweet.csv");
        dfEventTweets.sortBy("eventID").show();
        return  true;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }


    public static boolean detectAndSaveEvents(DataFrame<Object> dfTweets2017,DataFrame<Object> dfLambdas,String savePath,Long minTime,Long maxTime,Long lastTime,Integer timeBlock){
        DataFrame<Object> dfResults = new DataFrame<Object>("algo", "poiID", "timePeriod","eventSignal", "eventDetected");
        DataFrame<Object> dfDetectedEvents = new DataFrame<Object>("algo", "poiID", "eventSignal","timeStarted","prevTime","timeEnded");
        Long start_time1= System.currentTimeMillis();
//******************* Detection start from here
        String col_name = "algo,poiID,timePeriod,eventSignal,eventDetected";
        appendToCSV(col_name,savePath,"0915tmp_results.csv");
        col_name = "algo,poiID,eventSignal,timeStarted,prevTime,timeEnded";
        appendToCSV(col_name,savePath,"0915dfDetectedEvents.csv");




        List all_poiIDs_obj = dfTweets2017.unique("poiID").col("poiID");
        ArrayList<Integer> all_poiIDs = new ArrayList<Integer>();
        for(int i = 0;i<all_poiIDs_obj.size();i++){
            all_poiIDs.add(Integer.parseInt(all_poiIDs_obj.get(i).toString()));
        }

        List unixtimeMinList_obj = dfTweets2017.col("unixtimeMin");

        Long num_intervals = ((maxTime-minTime)/timeBlock);

        Integer numberOfRows = all_poiIDs.size();

        Long iters = new Long(0);

        // current timestamp in millis

        ArrayList<Integer> rows = new ArrayList<Integer>();
        for (int i = 0; i<numberOfRows;i++){
            rows.add(i);
        }
        List<String> columns = Arrays.asList("algo", "poiID", "timePeriod","eventSignal", "eventDetected");



        for(Long i=lastTime;i<maxTime;i+=timeBlock){
            //Start loop1
            Long start_time= System.currentTimeMillis();
            iters++;

            Integer row = -1;
//                dfResults = pd.DataFrame(index = np.arange(0, numberOfRows) ,columns = ['algo', 'poiID', 'timePeriod','eventSignal', 'eventDetected'])
            dfResults = new DataFrame<Object>(rows,columns);

            for(Integer tempPOIID : all_poiIDs){
                //Start loop2
                double signalProbThreshold = 0.01;
                Long currWindow = i;
                Integer same_poiID_lam_index = new Integer(0);
                //same_poiID_lam = dfLambdas['poiID']==tempPOIID
                ArrayList<Boolean> same_poiID_lam = new ArrayList<Boolean>();
                for (int t =0;t<dfLambdas.col("poiID").size();t++){
                    if (tempPOIID.equals(Integer.parseInt(dfLambdas.col("poiID").get(t).toString()))){
                        same_poiID_lam_index = tempPOIID-1;
                    }
                }

                DataFrame<Object> dfTweetsNow = new DataFrame<Object>();
                for(int t = 0;t<dfTweets2017.length();t++){
                    if(Long.parseLong(dfTweets2017.col("unixtimeMin").get(t).toString())<currWindow+timeBlock
                            &&Long.parseLong(dfTweets2017.col("unixtimeMin").get(t).toString())>=currWindow
                            &&tempPOIID.equals(Integer.parseInt(dfTweets2017.col("poiID").get(t).toString()))){
                        dfTweetsNow.append(dfTweets2017.row(t));
                    }
                }
                // get all tweet send in currentWindows
                Integer tweetsNowCount = dfTweetsNow.length();

                Double lambda_totalTweets = Double.parseDouble(dfLambdas.get(same_poiID_lam_index,1).toString());
                Double lambda_firstTweetTime = Double.parseDouble(dfLambdas.get(same_poiID_lam_index,2).toString());
                Double tempLambda = currWindow-lambda_firstTweetTime;
                tempLambda = tempLambda/timeBlock;
                tempLambda = lambda_totalTweets/tempLambda;
                /*
                currentWindow = 1483448400

                    1    57.0
                    Name: totalTweets, dtype: float64
                    1    1.483189e+09
                    Name: firstTweetTime, dtype: float64
                    1    0.131944
                    dtype: float64

                    2    2.0
                    Name: totalTweets, dtype: float64
                    2    1.483229e+09
                    Name: firstTweetTime, dtype: float64
                    2    0.005464
                    dtype: float64

                    3    1.0
                    Name: totalTweets, dtype: float64
                    3    1.483189e+09
                    Name: firstTweetTime, dtype: float64
                    3    0.002315
                    dtype: float64

                    4    6.0
                    Name: totalTweets, dtype: float64
                    4    1.483205e+09
                    Name: firstTweetTime, dtype: float64
                    4    0.014778
                    dtype: float64
                    ... ...
                * */
                // tempLambda here is the mean value, now generate a poisson distribution by this mean value
                // and get cdf of tweetsNowCount in this poisson distribution
                boolean eventDetected = false;
                Double eventSignal;

                PoissonDistribution poisson = new PoissonDistribution(tempLambda);
                eventSignal = 1 - poisson.cumulativeProbability(tweetsNowCount);

                /*
                eventSignal for iter == 1:
                0.1296752741666094
                0.005449577756994284
                0.0023121376970770546
                0.014669661623178731
                0.004866170422787031
                ... ...
                */
                Integer tempCount = Integer.parseInt(dfLambdas.col("totalTweets").get(same_poiID_lam_index).toString());
                tempCount+=tweetsNowCount;
                dfLambdas.set(same_poiID_lam_index,1,tempCount);

                //Start If-else
                //"prevTime","timeEnded"
                // 4 , 5
                if (eventSignal<signalProbThreshold && tweetsNowCount>=5){
                    eventDetected = true;
                    boolean isNew = true;
                    for (int k=0;k<dfDetectedEvents.length();k++){
                        Long tempTime = currWindow-timeBlock;
                        if (dfDetectedEvents.col("algo").get(k).toString().replaceAll("\\[", "").replaceAll("\\]","").equals("tweetVol")
                                && dfDetectedEvents.col("poiID").get(k).toString().replaceAll("\\[", "").replaceAll("\\]","").equals(tempPOIID.toString())
                                && dfDetectedEvents.col("prevTime").get(k).toString().replaceAll("\\[", "").replaceAll("\\]","").equals(tempTime.toString()))
                            {
                            System.out.println("event during time expanded!");
                            dfDetectedEvents.set(k,5,currWindow+timeBlock);
                            dfDetectedEvents.set(k,4,currWindow);
                            isNew=false;
                            appendToCSV("event during time expanded by following event",savePath,"dfDetectedEvents.csv");
                        }
                    }

                    if (isNew){
                    dfDetectedEvents.append(Arrays.asList("tweetVol",tempPOIID,eventSignal,currWindow,currWindow,currWindow+timeBlock));
                    Long tempLong = currWindow+timeBlock;
                    String rowstr = "tweetVol"+","+tempPOIID+","+eventSignal+","+currWindow+","+currWindow+","+tempLong;
                    appendToCSV(rowstr,savePath,"0915dfDetectedEvents.csv");}

                    System.out.println("******************Event detected at POI " + tempPOIID + " at time " + currWindow + "\n");
                }
                row++;
                dfResults.append(Arrays.asList("tweetVol", tempPOIID, i, eventSignal,eventDetected));
                String rowstr="tweetVol"+","+tempPOIID+","+i+","+eventSignal+","+eventDetected;
                appendToCSV(rowstr,savePath,"0915tmp_results.csv");

            }
            System.out.println("Iter # " + iters
                    + ", With time starts at " + i + " , Completed in " + (System.currentTimeMillis()-start_time) + " ms");

        }
//          ************* end of detection

        System.out.println("All finished in " + (System.currentTimeMillis()-start_time1) + " ms");
        return true;
    }

    public static long unixtimeMin(String melbTime_created_at){
        DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss +1100 yyyy",Locale.ENGLISH);
        try{
            Date date = dateFormat.parse(melbTime_created_at);
            long unixTime = (long) date.getTime()/1000;
            long unixTimeMin = unixTime-unixTime%600;
            return unixTimeMin;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    public static boolean appendToCSV(String currentEvent, String savePath, String fileName){
        boolean isSuccessful = false;
//        currentEvent.writeCsv(OutputStream os);
        String tempCsvPath  = savePath + fileName;
        File file = new File(tempCsvPath);
        try {
            if(!file.exists()){
                file.createNewFile();
                System.out.println("Create a new file");
            }
            FileOutputStream fos = new FileOutputStream(file,true);
            currentEvent = currentEvent.replaceAll("[\\[\\]]","")+"\n";
            fos.write(currentEvent.getBytes());



        }catch (Exception e){
            e.printStackTrace();

        }
        return  isSuccessful;
    }
}

