window.$ = window.jQuery;

window.stump = {};

stump.query = {

    advanced: function(ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-query-filters').addClass('stump-query-show-advanced');
        } else {
            $('#stump-query-filters').removeClass('stump-query-show-advanced');
        }
    },

    toggle: function(ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-rt-events').addClass('stump-show-' + $e.data('name'));
        } else {
            $('#stump-rt-events').removeClass('stump-show-' + $e.data('name'));
        }
    },

    initEvents: function() {
        var $toggle = $('.stump-toggle');
        var $toggleOn = $('.stump-toggle-on');

        $toggle.prop('checked', false);
        $toggleOn.prop('checked', true);
        $toggle.each(function(index, ele) {
            stump.query.toggle(ele);
        });
    }
};

stump.realtime = {

    input: {},
    lastBucketIndex: -1,
    chart: null,
    requireFocus: true,
    eventsBody: null,

    fillColors: [
        "rgba(220,220,220,0.5)",
        "rgba(151,187,205,0.5)",
        "rgba(205,151,187,0.5)",
        "rgba(187,205,151,0.5)",
        "rgba(151,205,187,0.5)",
        "rgba(205,187,151,0.5)",
        "rgba(187,151,205,0.5)"
    ],
    strokeColors: [
        "rgba(220,220,220,0.8)",
        "rgba(151,187,205,0.8)",
        "rgba(205,151,187,0.8)",
        "rgba(187,205,151,0.8)",
        "rgba(151,205,187,0.8)",
        "rgba(205,187,151,0.8)",
        "rgba(187,151,205,0.8)"
    ],
    highlightFills: [
        "rgba(220,220,220,0.75)",
        "rgba(151,187,205,0.75)",
        "rgba(205,151,187,0.75)",
        "rgba(187,205,151,0.75)",
        "rgba(151,205,187,0.75)",
        "rgba(205,187,151,0.75)",
        "rgba(187,151,205,0.75)"
    ],
    highlightStrokes: [
        "rgba(220,220,220,1)",
        "rgba(151,187,205,1)",
        "rgba(205,151,187,1)",
        "rgba(187,205,151,1)",
        "rgba(151,205,187,1)",
        "rgba(205,187,151,1)",
        "rgba(187,151,205,1)"
    ],

    init: function () {
        stump.realtime.eventsBody = $('#stump-rt-events > tbody');

        $waveform = $('#stump-rt-poll');
        stump.realtime.input.tenantId = $waveform.data('tenantId');
        stump.realtime.input.startTimestamp = new Date().getTime();
        stump.realtime.input.lookbackSeconds = parseInt($waveform.data('lookbackSeconds'));
        stump.realtime.input.buckets = parseInt($waveform.data('buckets'));
        stump.realtime.input.field1 = $waveform.data('field1');
        stump.realtime.input.terms1 = $waveform.data('terms1');
        stump.realtime.input.field2 = $waveform.data('field2');
        stump.realtime.input.terms2 = $waveform.data('terms2');
        stump.realtime.input.filters = $waveform.data('filters');

        stump.realtime.requireFocus = $waveform.data('requireFocus') != "false";
        stump.realtime.graphType = $waveform.data('graphType');
        stump.realtime.graphProp = (stump.realtime.graphType == 'Line' || stump.realtime.graphType == 'Radar') ? 'points'
            : (stump.realtime.graphType == 'Bar' || stump.realtime.graphType == 'StackedBar') ? 'bars'
            : 'unknown';

        if (stump.realtime.requireFocus) {
            miru.onWindowFocus.push(function () {
                if (stump.realtime.chart) {
                    stump.realtime.chart.update();
                }
            });
        }

        stump.realtime.poll();
    },

    poll: function () {
        $.ajax({
            type: "POST",
            url: "/stumptown/query/poll",
            data: {
                tenantId: stump.realtime.input.tenantId,
                startTimestamp: stump.realtime.input.startTimestamp,
                lookbackSeconds: stump.realtime.input.lookbackSeconds,
                buckets: stump.realtime.input.buckets,
                field1: stump.realtime.input.field1,
                terms1: stump.realtime.input.terms1,
                field2: stump.realtime.input.field2,
                terms2: stump.realtime.input.terms2,
                filters: stump.realtime.input.filters
            },
            //contentType: "application/json",
            success: function (data) {
                stump.realtime.update(data);
            },
            error: function () {
                //TODO error message
                console.log("error!");
            }
        });
    },

    update: function (data) {
        var i;
        if (data.waveforms) {
            if (!stump.realtime.chart) {
                var ctx = $('#stump-rt-canvas')[0].getContext("2d");
                var chartData = {
                    labels: [],
                    datasets: []
                };
                var secsPerBucket = stump.realtime.input.lookbackSeconds / stump.realtime.input.buckets;
                for (i = 0; i < stump.realtime.input.buckets; i++) {
                    var secsAgo = secsPerBucket * (stump.realtime.input.buckets - i);
                    chartData.labels.push(stump.realtime.elapsed(Math.round(secsAgo)));
                }
                i = 0;
                $.each(data.waveforms, function (key, value) {
                    chartData.datasets.push({
                        label: key,
                        fillColor: stump.realtime.fillColors[i % stump.realtime.fillColors.length],
                        strokeColor: stump.realtime.strokeColors[i % stump.realtime.strokeColors.length],
                        highlightFill: stump.realtime.highlightFills[i % stump.realtime.highlightFills.length],
                        highlightStroke: stump.realtime.highlightStrokes[i % stump.realtime.highlightStrokes.length],
                        data: value
                    });
                    i++;
                });
                stump.realtime.chart = (new Chart(ctx))[stump.realtime.graphType](chartData, {
                    multiTooltipTemplate: "<%= datasetLabel %> - <%= value %>",
                    legendTemplate: "<ul style=\"list-style-type:none; margin:20px 0 0 0;\"><% for (var i=0; i<datasets.length; i++){%><li style=\"display:inline-block;\"><span style=\"background-color:<%=datasets[i].strokeColor%>; width:16px; height:16px; display:inline-block; margin:4px; vertical-align:middle;\"></span><%if(datasets[i].label){%><%=datasets[i].label%><%}%></li><%}%></ul>",
                    scaleLineColor: "rgba(128,128,128,0.5)",
                    tooltipFillColor: "rgba(0,0,0,1)",
                    pointDot: false,
                    bezierCurve: true,
                    bezierCurveTension: 0.4,
                    datasetFill: false,
                    responsive: true,
                    animation: false
                });
                $('#stump-rt-legend').html(stump.realtime.chart.generateLegend());
            }
            //data.startBucketIndex;
            //data.elapse;
            i = 0;
            $.each(data.waveforms, function (key, value) {
                if (i < stump.realtime.chart.datasets.length) {
                    for (var j = 0; j < value.length; j++) {
                        stump.realtime.chart.datasets[i][stump.realtime.graphProp][j].value = value[j];
                    }
                }
                i++;
            });
            if (!stump.realtime.requireFocus || miru.windowFocused) {
                stump.realtime.chart.update();
            }
        }
        if (data.logEvents) {
            $('tr.stump-rt-log-event').remove();
            for (i = 0; i < data.logEvents.length; i++) {
                stump.realtime.eventsBody.append(data.logEvents[i]);
            }
        }
        setTimeout(stump.realtime.poll, 1000);
    },

    elapsed: function (seconds) {
        var years, months, days, hours, minutes;
        if (seconds < 0) {
            return '0s';
        }
        if (seconds < 60) {
            return seconds + 's';
        }
        if (seconds < 60 * 60) {
            minutes = Math.floor(seconds / 60);
            seconds -= minutes * 60;
            if (seconds > 0) {
                return minutes + 'm ' + seconds + 's';
            } else {
                return minutes + 'm';
            }
        }
        if (seconds < 60 * 60 * 24) {
            hours = Math.floor(seconds / 60 / 60);
            seconds -= hours * 60 * 60;
            minutes = Math.floor(seconds / 60);
            if (minutes > 0) {
                return hours + 'h ' + minutes + 'm';
            } else {
                return hours + 'h';
            }
        }
        if (seconds < 60 * 60 * 24 * 31) {
            days = Math.floor(seconds / 60 / 60 / 24);
            seconds -= days * 60 * 60 * 24;
            hours = Math.floor(seconds / 60 / 60);
            if (hours > 0) {
                return days + 'd ' + hours + 'h';
            } else {
                return days + 'd';
            }
        }
        if (seconds < 60 * 60 * 24 * 365) {
            months = Math.floor(seconds / 60 / 60 / 24 / 31);
            seconds -= months * 60 * 60 * 24 * 31;
            days = Math.floor(seconds / 60 / 60 / 24);
            if (days > 0) {
                return months + 'mo ' + days + 'd';
            } else {
                return months + 'mo';
            }
        }
        years = Math.floor(seconds / 60 / 60 / 24 / 365);
        seconds -= years * 60 * 60 * 24 * 365;
        months = Math.floor(seconds / 60 / 60 / 24 / 31);
        if (months > 0) {
            return years + 'y ' + months + 'mo';
        } else {
            return years + 'y';
        }
    }
};

$(document).ready(function() {
    stump.query.initEvents();
});