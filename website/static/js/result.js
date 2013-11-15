var ResultTimeline = (function(window){

// Localize globals
var readCheckbox = window.readCheckbox, getLoadText = window.getLoadText, valueOrDefault = window.valueOrDefault;

function renderPlot(data, div_id) {
  var plotoptions = {
    title: {text: data.metric, fontSize: '1.1em'},
    axes:{
      yaxis:{
        label: data.units + " " + data.lessisbetter,
        labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
        min: 0, autoscale:true,
      },
      xaxis:{
        // renderer: $.jqplot.DateAxisRenderer,
        label: 'Time',
        labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
        //tickRenderer: $.jqplot.CanvasAxisTickRenderer,
        //tickOptions:{formatString:'%b %d', angle:-40},
        pad: 1.01,
        autoscale:true,
        min: 0,
        //rendererOptions:{sortMergedLabels:true} /* only relevant when
        //                        $.jqplot.CategoryAxisRenderer is used */
      }
    },
    legend: {show: false},
    highlighter: {
        show: true,
        tooltipLocation: 'nw',
        yvalues: 2,
        formatString:'<table class="jqplot-highlighter"><tr><td>time:</td><td>%s</td></tr> <tr><td>' + data.metric + ':</td><td>%s</td></tr></table>'
    },
    cursor: {show: true, zoom:true, showTooltip:false, clickReset:true}
  };

  $("#" + div_id).html('<div id="' + div_id + '_plot"></div><div id="plotdescription"></div>');
  //Render plot
  $.jqplot(div_id + '_plot', [data.data], plotoptions);
}

function render() {
    $("#plotgrid").html("");
    // render single plot when one benchmark is selected
    $("input[name^='metric']:checked").each(function() {
        var metric = $(this).val();
        var plotid = "plot_" + metric;
        $("#plotgrid").append('<div id="' + plotid + '" class="plotcontainer"></div>');
        renderPlot(defaults.data[metric], plotid);
    });
}

function getConfiguration() {
  var config = {
    id: defaults.result,
    met: readCheckbox("input[name='metric']:checked"),
  };
  return config;
}

function refreshContent() {
    render();
}

function updateUrl() {
  var cfg = getConfiguration();
  $.address.autoUpdate(false);
  for (var param in cfg) {
    $.address.parameter(param, cfg[param]);
  }
  $.address.update();
}

function initializeSite(event) {
    setValuesOfInputFields(event);
    $("input[name='metric']"      ).on('click', updateUrl);
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
