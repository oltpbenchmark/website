var BenchmarkBar = (function(window){

function renderPlot(data, div_id) {
    var plotoptions = {
        title: {text: data.metric + " " + data.lessisbetter, fontSize: '1.1em'},
        seriesDefaults:{
            renderer:$.jqplot.BarRenderer,
            rendererOptions: {
                varyBarColor: true,
            },
            pointLabels: { show: true }
        },
        axes:{
            yaxis:{
                label: data.unit,
                labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
                min: 0, autoscale:true,
            },
            xaxis:{
                renderer: $.jqplot.CategoryAxisRenderer,
                label: 'DBMS Configuration',
                labelRenderer: $.jqplot.CanvasAxisLabelRenderer,
                ticks: data.tick,
                pad: 1.01,
                autoscale:true,
            }
        },
        cursor: {show: true, zoom:true, showTooltip:false, clickReset:true},
    };

    $("#" + div_id).html('<div id="' + div_id + '_plot"></div><div id="plotdescription"></div>');
    var plot = $.jqplot(div_id + '_plot', data.data, plotoptions);

    $('#' + div_id + '_plot').bind('jqplotDataHighlight',
        function (ev, seriesIndex, pointIndex, pointed_data ) {
            $('#chartpseudotooltip').html('<table class="jqplot-highlighter-tooltip"><tr><td>Result ID:</td><td>' + pointed_data[2] + '</td></tr> <tr><td>' + data.metric + ':</td><td>' + pointed_data[1].toFixed(2) + '</td></tr></table>');
            x = plot.axes.xaxis.u2p(pointed_data[0]),  // convert x axis unita to pixels
            y = plot.axes.yaxis.u2p(pointed_data[1]);  // convert y axis units to pixels
            var mouseX = $('#' + div_id + '_plot').position().left + x;
            var mouseY = $('#' + div_id + '_plot').position().top + y - $('#chartpseudotooltip').height();
            var cssObj = {
                'position' : 'absolute',
                'left' : mouseX + 'px',
                'top' : mouseY + 'px'
            };
            $('#chartpseudotooltip').css(cssObj);
        }
    );

    $('#' + div_id + '_plot').bind('jqplotDataUnhighlight',
        function (ev) {
            $('#chartpseudotooltip').html('');
        }
    );
}

function render(data) {
    $("#plotgrid").html("");

    if(data.error !== "None") {
        var h = $("#content").height();
        $("#plotgrid").html(getLoadText(data.error, h, false));
    } else if (data.results[0].data.length === 0) {
        var h = $("#content").height();
        $("#plotgrid").html(getLoadText("No data available", h, false));
    } else {
        for (var metric in data.results) {
            var plotid = "plot_" + metric;
            $("#plotgrid").append('<div id="' + plotid + '" class="plotcontainer"></div>');
            renderPlot(data.results[metric], plotid);
        }
    }
}

function OnMarkerClickHandler(ev, gridpos, datapos, neighbor, plot) {
    if (neighbor) {
        result_id = neighbor.data[2];
        // return '<a href="/projects/' + defaults.proj + '/apps/' + defaults.app + '/results/' + data + '">' + data + '</a>';
//        window.location = "/result/?id=" + result_id;
        window.location = '/projects/' + defaults.proj + '/apps/' + defaults.app + '/results/' + result_id;
    }
}

function getConfiguration() {
    var config = {
        id: defaults.benchmark,
        db: readCheckbox("input[name^='db_']:checked"),
        met: readCheckbox("input[name='metric']:checked"),
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
    $("input[name='db']"          ).bind('click', updateUrl);
    $("input[name='db']"          ).on('change', updateSub);
    $("input[name^='db_']"        ).on('click', updateUrl);
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

    // Set default selected db
    $("input:checkbox[name^='db_']").removeAttr('checked');
    var dbs = event.parameters.db ? event.parameters.db.split(',') : defaults.db_confs;
    var sel = $("input[name^='db_']");
    $.each(dbs, function(i, db) {
        sel.filter("[value='" + db + "']").prop('checked', true);
    });

    // Set selected db type
    $("input:checkbox[name='db']").removeAttr('checked');
    $("input[name='db']").each(function() {
        $(this).prop('checked', allChecked("input[name='db_" + $(this).val() + "']"));
    });
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
