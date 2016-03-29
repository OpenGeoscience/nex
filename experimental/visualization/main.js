// Run after the DOM loads
$(function () {
  'use strict';

  // Utility method to create a point layer for winkler dataset (for now)
  // A value of 2000 is hard-coced based on the data
  function makePointLayer(geoData, layer) {
    layer
      .createFeature('point')
      .data(geoData)
      .position(function(d) { return {x: parseFloat(d.lon),
                                      y: parseFloat(d.lat)}})
      .style('radius', function(d) {
        return (parseFloat(d.degree_days)/2000.0);
      });
  }

  function processDataAndRender(layer) {
    // Load the data
    $.ajax({
      url: 'winkler_scale_IPSL-CM5A-LR_1997.csv',
      dataType: 'text',
      success: function (geoData) {

        var record_num = 5;  // or however many elements there are in each row
        var allTextLines = geoData.split(/\r\n|\n/);
        var entries = allTextLines[0].split(',');
        var index = 1;
        var lines = [];

        var headings = entries.splice(0, record_num);
        entries = allTextLines[1].split(',');
        while (entries.length > 0 && index < (allTextLines.length - 1)) {
            var keyvalue = {};
            for (var j=0; j < record_num; j++) {
                keyvalue[headings[j]] = entries.shift();
            }
            lines.push(keyvalue);
            index += 1;
            entries = allTextLines[index].split(',');
        }

        makePointLayer(lines, layer);

        // Draw the map
        map.draw();
      }
    });
  }

  var map = geo.map({
    node: '#map',
    center: {
      x: -98.0,
      y: 39.5
    },
    zoom: 3
  });

  map.createLayer(
    'osm',
    {
      baseUrl: 'http://otile1.mqcdn.com/tiles/1.0.0/map/'
    }
  );

  var vglLayer = map.createLayer(
    'feature',
    {
      renderer: 'vgl'
    }
  );

  processDataAndRender(vglLayer);

  map.draw();
});
