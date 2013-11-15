var BenchmarkBar = (function(window){

// Localize globals
var readCheckbox = window.readCheckbox, getLoadText = window.getLoadText, valueOrDefault = window.valueOrDefault;

function renderPlot(data, div_id) {
  var plotdata = [],
      series = [],
      lastvalues = [];//hopefully the smallest values for determining significant digits.
  seriesindex = [];
  for (var branch in data.branches) {
    // NOTE: Currently, only the "default" branch is shown in the timeline
    for (var exe_id in data.branches[branch]) {
      series.push({"label":  exe_id});
      seriesindex.push(exe_id);
      plotdata.push(data.branches[branch][exe_id]);
      lastvalues.push(data.branches[branch][exe_id][0][1]);
    }
    //determine significant digits
    var digits = 2;
    var value = Math.min.apply( Math, lastvalues );
    if (value !== 0) {
      while( value < 1 ) {
        value *= 10;
        digits++;
      }
    }


    if (data.benchmark_description) {
      $("#plotdescription").html('<p class="note"><i>' + data.benchmark + '</i>: ' + data.benchmark_description + '</p>');
    }
  }

  var plotoptions = {
    title: {text: data.metric, fontSize: '1.1em'},
    // series: series,
    seriesDefaults:{
        renderer:$.jqplot.BarRenderer,
        rendererOptions: {
            varyBarColor: true,
            // barWidth: 10,
        },
        pointLabels: { show: true }
    },
    axes:{
        yaxis:{
            label: data.unit + " " + data.lessisbetter,
            labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
            min: 0, autoscale:true,
        },
      xaxis:{
        renderer: $.jqplot.CategoryAxisRenderer,
        label: 'DB Conf ID',
        labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
        ticks: data.tick,
        pad: 1.01,
        autoscale:true,
      }
    },
    legend: {show: false, location: 'nw'},
    highlighter: {
        show: true,
        tooltipLocation: 'n',
        showMarker: false,
        // yvalues: 2,
        formatString:'<table class="jqplot-highlighter"><tr><td>DBConf ID:</td><td>%s</td></tr> <tr><td>' + data.metric + ':</td><td>%s</td></tr></table>'
    },
    cursor: {show: true, zoom:true, showTooltip:false, clickReset:true}
  };
  if (series.length > 4) {
      // Move legend outside plot area to unclutter
      var labels = [];
      for (var l in series) {
          labels.push(series[l].label.length);
      }
      var offset = 55 + Math.max.apply( Math, labels ) * 5.4;
      plotoptions.legend.location = 'ne';
      plotoptions.legend.xoffset = -offset;
      $("#plot").css("margin-right", offset + 10);
      var w = $("#plot").width();
      $("#plot").css('width', w - offset);
  }
  $("#" + div_id).html('<div id="' + div_id + '_plot"></div><div id="plotdescription"></div>');
  //Render plot
  $.jqplot(div_id + '_plot', [data.data], plotoptions);
}

function render(data) {
    $("#plotgrid").html("");
    // render single plot when one benchmark is selected
    for (var metric in data.results) {
        var plotid = "plot_" + metric;
        $("#plotgrid").append('<div id="' + plotid + '" class="plotcontainer"></div>');
        renderPlot(data.results[metric], plotid);
    }
}

function getConfiguration() {
  var config = {
    id: defaults.benchmark,
    db: readCheckbox("input[name^='db_']:checked"),
    ben: $("input[name='benchmark']:checked").val(),
    spe: readCheckbox("input[name^='specific']:checked"),
    met: readCheckbox("input[name='metric']:checked"),
    revs: $("#revisions option:selected").val(),
    equid: $("#equidistant").is(':checked') ? "on" : "off"
  };
  return config;
}

function refreshContent() {
  var h = $("#content").height();//get height for loading text
  $("#plotgrid").fadeOut("fast", function() {
    $("#plotgrid").html(getLoadText("Loading...", h, true)).show();
    $.getJSON("/get_benchmark_data/", getConfiguration(), render);
  });
}

function updateUrl() {
  var cfg = getConfiguration();
  $.address.autoUpdate(false);
  for (var param in cfg) {
    $.address.parameter(param, cfg[param]);
  }
  $.address.update();
}

function updateSub(event) {
    var db_name = event.target.value;
    $("input[name='db_" + db_name + "']").each( function() {
        $(this).prop('checked', event.target.checked);
    });
}

function initializeSite(event) {
    setValuesOfInputFields(event);
    $("#revisions"                ).bind('change', updateUrl);
    $("input[name='db']"          ).bind('click', updateUrl);
    $("input[name='db']"          ).on('change', updateSub);
    $("input[name^='db_']"        ).on('click', updateUrl);
    $("input[name^='specific']"   ).on('change', updateUrl);
    $("select[name^='additional']").bind('change', updateUrl);
    $("input[name='metric']"      ).on('click', updateUrl);
    $("#equidistant"              ).bind('change', updateUrl);
}

function refreshSite(event) {
    setValuesOfInputFields(event);
    refreshContent();
}

function setValuesOfInputFields(event) {
    // Either set the default value, or the one parsed from the url

    // Set default selected metrics
    $("input:checkbox[name='metric']").prop('checked', false);
    var metrics = event.parameters.met ? event.parameters.met.split(',') : defaults.metrics;
    $("input:checkbox[name='metric']").each(function() {
        if ($.inArray($(this).val(), metrics) >= 0) {
            $(this).prop('checked', true);
        }
    });

    // Set default selected db
    $("input:checkbox[name^='db_']").removeAttr('checked');
    var dbs = event.parameters.db ? event.parameters.db.split(',') : defaults.db_confs;
    var sel = $("input[name^='db_']");
    $.each(dbs, function(i, db) {
        sel.filter("[value='" + db + "']").prop('checked', true);
    });
}

function init(def) {
    defaults = def;

    $.ajaxSetup ({
      cache: false
    });

    // Event listener for clicks on plot markers
    // $.jqplot.eventListenerHooks.push(['jqplotClick', OnMarkerClickHandler]);

    // Init and change handlers are set to the refreshContent handler
    $.address.init(initializeSite).change(refreshSite);
}

return {
    init: init
};

})(window);
