<!-- AIzaSyBGBCXhUvTKsg6NUO4LCnq24P7kgvqRbn0 -->
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
        height: 500px; width:1000px;
      }
      /* Optional: Makes the sample page fill the window. */
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
    </style>
    <script src="https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/markerclusterer.js">
</script>
    <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBGBCXhUvTKsg6NUO4LCnq24P7kgvqRbn0&callback=initMap"
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
        var labels = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        var data;


        var locations=[];
        
      
  
      function handleFileSelect(evt) {
        var file = evt.target.files[0];
     
        Papa.parse(file, {
          header: true,
          dynamicTyping: true,
          complete: function(results) {
            data = results.data;
            

            for (i = 0,len = data.length;i<len;i++){
                console.log(data[i]);

                if(data[i].hasOwnProperty("lat")){

                // var marker = new google.maps.Marker({position: {lat: data[i].lat, lng: data[i].long}, map: map});
                    locations.push({lat: data[i].lat,lng: data[i].long});
                }

                var markers = locations.map(function(location, i) {
          return new google.maps.Marker({
            position: location,
            label: 'event',
            icon: "./placeholder.png"
          });
        });

        var markerCluster = new MarkerClusterer(map, markers,
            {imagePath: 'https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m'});

            }

            // console.log(data["0"]);

          }
        });
      }
     
      $(document).ready(function(){
        $("#csv-file").change(handleFileSelect);
      });


    </script>
 
    <input type="file" id="csv-file" name="files"/>


  </body>
</html>
