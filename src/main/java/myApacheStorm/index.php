<!-- select a file to read and print out the events -->
<!DOCTYPE html>
<html>
  <head>
    <title>Simple Map</title>
    <meta name="viewport" content="initial-scale=1.0">
    <meta charset="utf-8">
    <style>
      /* Always set the map height explicitly to define the size of the div
       * element that contains the map. */
      #map {
        height: 90%;       }
      /* Optional: Makes the sample page fill the window. */
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
    </style>
    <script src="https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/markerclusterer.js">
</script>
    <script src="https://maps.googleapis.com/maps/api/js?key=YOUR_KEY_HERE&callback=initMap"
    async defer></script>
    <script type="text/javascript" src="./papaparse.min.js"></script>
    <script
      src="https://code.jquery.com/jquery-2.2.4.min.js"
      integrity="sha256-BbhdlvQf/xTY9gja0Dq3HiwQF8LaCRTXxZKRutelT44="
      crossorigin="anonymous"></script>

  </head>
  <body>
    <div id="map"></div>
    <script>
        //initial map
      var map;
      function initMap() {
        map = new google.maps.Map(document.getElementById('map'), {
          center: {lat: -37.81464473927567, lng: 144.97559213362672},
          // melb center: {lat: -37.815018, lng: 144.946014}
          zoom: 12
        });
        
      }
      var data;

      var markers=[];

      var events=[];

      var tweets;

        
      
  
      function handleFileSelect(evt) {
        var file = evt.target.files[0];
     
        Papa.parse(file, {
          header: true,
          dynamicTyping: true,
          complete: function(results) {
            data = results.data;
            

            for (i = 0,len = data.length;i<len;i++){
              if(data[i].hasOwnProperty("lat") && 
                (data[i].Full_steps == "MaybeEvent" || data[i].Full_steps == "GoodEvent")){
                  marker = new google.maps.Marker({
                    position: new google.maps.LatLng(data[i].lat, data[i].long),
                    map: map,
                    label: data[i].eventID.toString()

                    });
                  markers.push(marker);

                } 

            }//end of for loop



            var markerCluster = new MarkerClusterer(map, markers,
              {imagePath: 'https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m'});

            
            for (marker in markerCluster.getMarkers()){
              marker.addListener('click', function() {
                console.log("addListener");
              });
            }

            google.maps.event.addListener(markerCluster,'clusterclick',function(){
              var currentMarkers = markerCluster.getMarkers();

              for (var i=0; i < markers.length; i++) 
                {
                    if (map.getBounds().contains(markers[i].getPosition()))
                    {
                        
                        events.push(parseInt(markers[i].getLabel(), 10));
                    } 
                }

              for(var i =0;i<events.length;i++){
                console.log("print tweets of event "+events[i]);
                for (var j=0;j<tweets.length;j++){
                  if (tweets[j].eventID==events[i]){

                  }
                }
              }
              events=[];


              console.log("\n\n\n");

            });//end of listener for marker Cluster

              }//end of complete function
            });//end of Papa Parser function
      }//end of handleFileSelect function
      function handleFileSelectTweet(evt) {
        var file = evt.target.files[0];
        Papa.parse
        (file,
          {
            header: true,
            dynamicTyping: true,
            complete: function(results) {
              data = results.data;
              tweets = data;
              console.log("Tweets loaded.");
            }
          }
        );
      }
     
      $(document).ready(function(){
        $("#csv-event-file").change(handleFileSelect);
        $("#csv-tweet-file").change(handleFileSelectTweet);
      });


    </script>
    <label>Select events file</label>
    <input type="file" id="csv-event-file" name="files"/>
    <label>Select tweets of events file</label>
    <input type="file" id="csv-tweet-file" name="files"/>


  </body>
</html>
