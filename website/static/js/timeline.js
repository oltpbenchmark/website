var Timeline = (function(window){

// Localize globals
var readCheckbox = window.readCheckbox, getLoadText = window.getLoadText, valueOrDefault = window.valueOrDefault;

var seriesindex = [],
    baselineColor = "#d8b83f",
    seriesColors = ["#4bb2c5", "#EAA228", "#579575", "#953579", "#839557", "#ff5800", "#958c12", "#4b5de4", "#0085cc"],
    defaults;

function shouldPlotEquidistant() {
  return $("#equidistant").is(':checked');
}

function OnMarkerClickHandler(ev, gridpos, datapos, neighbor, plot) {
    if($("input[name='benchmark']:checked").val() === "grid") { return false; }
    if (neighbor) {
        result_id = neighbor.data[3];
        window.location = "/result/?id=" + result_id;
    }
}

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
    $("#" + div_id).html('<div id="' + div_id + '_plot"></div><div id="plotdescription"></div>');

    if (data.benchmark_description) {
      $("#plotdescription").html('<p class="note"><i>' + data.benchmark + '</i>: ' + data.benchmark_description + '</p>');
    }
  }
  var plotoptions = {
    title: {text: data.benchmark + ": " + data.metric, fontSize: '1.1em'},
    series: series,
    axes:{
      yaxis:{
        label: data.units + " " + data.lessisbetter,
        labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
        min: 0, autoscale:true,
        tickOptions:{formatString:'%.' + digits + 'f'}
      },
      xaxis:{
        renderer: (shouldPlotEquidistant()) ? $.jqplot.CategoryAxisRenderer : $.jqplot.DateAxisRenderer,
        label: 'Date',
        labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
        tickRenderer: $.jqplot.CanvasAxisTickRenderer,
        tickOptions:{formatString:'%b %d', angle:-40},
        pad: 1.01,
        autoscale:true,
        rendererOptions:{sortMergedLabels:true} /* only relevant when
                                $.jqplot.CategoryAxisRenderer is used */ 
      }
    },
    legend: {show: true, location: 'nw'},
    highlighter: {
        show: true,
        tooltipLocation: 'nw',
        yvalues: 2,
        formatString:'<table class="jqplot-highlighter"><tr><td>date:</td><td>%s</td></tr> <tr><td>result:</td><td>%s</td></tr></table>'
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
  //Render plot
  $.jqplot(div_id + '_plot',  plotdata, plotoptions);
}

function renderMiniplot(plotid, data) {
  var plotdata = [],
      series = [];

  for (var branch in data.branches) {
    for (var id in data.branches[branch]) {
      series.push({
        "label": $("label[for*='executable" + id + "']").html()
      });
      plotdata.push(data.branches[branch][id]);
    }
  }
  if (data.baseline !== "None") {
    series.push({
      "color": baselineColor,
      showMarker: false,
      lineWidth: 1.5
    });
    plotdata.push(data.baseline);
  }

  var plotoptions = {
    title: {text: data.benchmark + ": " + data.metric, fontSize: '1.1em'},
    seriesDefaults: {lineWidth: 2, markerOptions:{style:'circle', size: 6}},
    series: series,
    axes: {
      yaxis: {
        min: 0, autoscale:true, showTicks: false
      },
      xaxis: {
        renderer:$.jqplot.DateAxisRenderer,
        pad: 1.01,
        autoscale:true,
        showTicks: false
      }
    },
    highlighter: {show:false},
    cursor:{showTooltip: false, style: 'pointer'}
  };
  $.jqplot(plotid, plotdata, plotoptions);
}

var fixed_header = null;

function render(data) {
  $("#revisions").attr("disabled", false);
  $('#revisions').selectpicker('refresh');
  $("#equidistant").attr("disabled", false);
  $("#plotgrid").html("");
  if(data.error !== "None") {
    var h = $("#content").height();//get height for error message
    $("#plotgrid").html(getLoadText(data.error, h, false));
    return 1;
  } else if ($("input[name='benchmark']:checked").val() === "show_none") {
    var h = $("#content").height();//get height for error message
    $("#plotgrid").html(getLoadText("Please select a benchmark on the left", h, false));
  } else if (data.timelines.length === 0) {
    var h = $("#content").height();//get height for error message
    $("#plotgrid").html(getLoadText("No data available", h, false));
  } else if ($("input[name='benchmark']:checked").val() === "grid"){
    //Render Grid of plots
    $("#revisions").attr("disabled",true);
    $('#revisions').selectpicker('refresh');
    $("#equidistant").attr("disabled", true);
    for (var bench in data.timelines) {
      var plotid = "plot_" + data.timelines[bench].benchmark;
      $("#plotgrid").append('<div id="' + plotid + '" class="miniplot"></div>');
      $("#" + plotid).click(function() {
        var bench = $(this).attr("id").slice(5);
        $("#benchmark_" + bench).trigger("click");//.prop("checked", true);
        updateUrl();
      });
      renderMiniplot(plotid, data.timelines[bench]);
    }
  } else {
    // render single plot when one benchmark is selected
    for (var metric in data.timelines) {
        var plotid = "plot_" + data.timelines[metric].metric;
        $("#plotgrid").append('<div id="' + plotid + '" class="plotcontainer"></div>');
        renderPlot(data.timelines[metric], plotid);
    }

  }
    var dt = $("#dataTable").dataTable( {
        "aaData": data.results,
        "aoColumns": [
            { "sTitle": "ID", "sClass": "center", "sType": "num-html", "mRender": function (data, type, full) {
                return '<a href="/result/?id=' + data + '">' + data + '</a>';
            }},
            { "sTitle": "Creation Time", "sClass": "center"},
            { "sTitle": "DB Conf", "sClass": "center", "mRender": function (data, type, full) {
                return '<a href="/db_conf/?id=' + full[6] + '">' + data + '</a>';
            }},
            { "sTitle": "Benchmark Conf", "sClass": "center", "mRender": function (data, type, full) {
                return '<a href="/benchmark_conf/?id=' + full[7] + '">' + data + '</a>';
            }},
            { "sTitle": "Throughput", "sClass": "center", "mRender": function (data, type, full) {return data.toFixed(2);}},
            { "sTitle": "p99 Latency", "sClass": "center", "mRender": function (data, type, full) {return data.toFixed(2);}},
        ],
        "bFilter": false,
        "bAutoWidth": false,
        "sPaginationType": "full_numbers",
        "bDestroy": true
    });
    if (fixed_header != null) {
        fixed_header.fnUpdate();
    } else {
        fixed_header = new FixedHeader(dt);
    }
}

function refreshContent() {
  var h = $("#content").height();//get height for loading text
  $("#plotgrid").fadeOut("fast", function() {
    $("#plotgrid").html(getLoadText("Loading...", h, true)).show();
    $.getJSON("/get_data/", getConfiguration(), render);
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

function getConfiguration() {
  var config = {
    proj: defaults.proj,
    db: readCheckbox("input[name='db']:checked"),
    ben: $("input[name='benchmark']:checked").val(),
    spe: readCheckbox("input[name^='specific']:checked"),
    met: readCheckbox("input[name='metric']:checked"),
    revs: $("#revisions option:selected").val(),
    equid: $("#equidistant").is(':checked') ? "on" : "off"
  };
  config["add"] = [];
  $.each(defaults.additional, function(i, add) {
    config["add"].push(add + ":" + $("select[name^='additional_" + add + "']").val());
  });

  return config;
}

function updateSub(event) {
    $("[id^=div_specific]").hide();
    $("input[name^='specific']").removeAttr('checked');
    if ($("input[name='benchmark']:checked").val() != "grid" && $("input[name='benchmark']:checked").val() != "show_none") {
        $("[id=div_specific_" + $("input[name='benchmark']:checked").val() + "]").show();
        $("[id^=specific_" + $("input[name='benchmark']:checked").val() + "_]").prop('checked', true);
    }
}

function initializeSite(event) {
    setValuesOfInputFields(event);
    $("#revisions"                ).bind('change', updateUrl);
    $("input[name='db']"          ).bind('click', updateUrl);
    $("input[name='benchmark']"   ).on('change', updateSub);
    $("input[name='benchmark']"   ).on('click', updateUrl);
    $("input[name^='specific']"   ).on('change', updateUrl);
    $("select[name^='additional']").bind('change', updateUrl);
    $("input[name='metric']"   ).on('click', updateUrl);
    $("#equidistant"              ).bind('change', updateUrl);
}

function refreshSite(event) {
    setValuesOfInputFields(event);
    refreshContent();
}

function setValuesOfInputFields(event) {
    // Either set the default value, or the one parsed from the url

    // Set default selected recent results
    $("#revisions").val(valueOrDefault(event.parameters.revs, defaults.revisions));
    $('#revisions').selectpicker('refresh');

    // Set default selected metrics
    $("input:checkbox[name='metric']").prop('checked', false);
    var metrics = event.parameters.met ? event.parameters.met.split(',') : defaults.metrics;
    $("input:checkbox[name='metric']").each(function() {
        if ($.inArray($(this).val(), metrics) >= 0) {
            $(this).prop('checked', true);
        }
    });

    // Set default selected db
    $("input:checkbox[name='db']").removeAttr('checked');
    var dbs = event.parameters.db ? event.parameters.db.split(',') : defaults.dbs;
    var sel = $("input[name='db']");
    $.each(dbs, function(i, db) {
        sel.filter("[value='" + db + "']").prop('checked', true);
    });

    // Set default selected benchmark
    var benchmark = valueOrDefault(event.parameters.ben, defaults.benchmark);
    $("input:radio[name='benchmark']").filter("[value='" + benchmark + "']").attr('checked', true);
    $("[id^=div_specific]").hide();
    $("input[name^='specific']").removeAttr('checked');
    if ($("input[name='benchmark']:checked").val() != "grid" && $("input[name='benchmark']:checked").val() != "show_none") {
        $("[id=div_specific_" + $("input[name='benchmark']:checked").val() + "]").show();
        sel = $("[id^=specific_" + $("input[name='benchmark']:checked").val() + "_");
        var specs = event.parameters.spe? event.parameters.spe.split(','): defaults.benchmarks[benchmark];
        $.each(specs, function(i, spec) {
            sel.filter("[value='" + spec + "']").prop('checked', true);
        });
    }

    // Set default selected additional filter
    if (event.parameters.add) {
        var filters = event.parameters.add.split(',');
        $.each(filters, function(i, filter) {
            var kv = filter.split(':');
            var name = kv[0];
            var value = kv[1];
            $("select[name^='additional_" + name + "']").val(value);
            $("select[name^='additional_" + name + "']").selectpicker('refresh');
        });
    } else {
        $.each(defaults.additional, function(i, add) {
            $("select[name^='additional_" + add + "']").val("select_all");
            $("select[name^='additional_" + add + "']").selectpicker('refresh');
        });
    }

    // Set equidistant status
    $("#equidistant").prop('checked', valueOrDefault(event.parameters.equid, defaults.equidistant) === "on");
}

function init(def) {
    defaults = def;

    $.ajaxSetup ({
      cache: false
    });

    // Event listener for clicks on plot markers
    $.jqplot.eventListenerHooks.push(['jqplotClick', OnMarkerClickHandler]);

    // Init and change handlers are set to the refreshContent handler
    $.address.init(initializeSite).change(refreshSite);
}

return {
    init: init
};

})(window);
