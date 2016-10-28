window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.realwave = {
    input: {},
    lastBucketIndex: -1,
    chart: null,
    requireFocus: true,
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
        $waveform = $('#rw-waveform');
        miru.realwave.input.tenantId = $waveform.data('tenantId');
        miru.realwave.input.startTimestamp = new Date().getTime();
        miru.realwave.input.lookbackSeconds = parseInt($waveform.data('lookbackSeconds'));
        miru.realwave.input.buckets = parseInt($waveform.data('buckets'));
        miru.realwave.input.field1 = $waveform.data('field1');
        miru.realwave.input.terms1 = $waveform.data('terms1');
        miru.realwave.input.field2 = $waveform.data('field2');
        miru.realwave.input.terms2 = $waveform.data('terms2');
        miru.realwave.input.filters = $waveform.data('filters');

        miru.realwave.requireFocus = $waveform.data('requireFocus') != "false";
        miru.realwave.graphType = $waveform.data('graphType');
        miru.realwave.graphProp = (miru.realwave.graphType == 'Line' || miru.realwave.graphType == 'Radar') ? 'points'
                : (miru.realwave.graphType == 'Bar' || miru.realwave.graphType == 'StackedBar') ? 'bars'
                : 'unknown';

        if (miru.realwave.requireFocus) {
            miru.onWindowFocus.push(function () {
                if (miru.realwave.chart) {
                    miru.realwave.chart.update();
                }
            });
        }

        miru.realwave.poll();
    },
    poll: function () {
        $.ajax({
            type: "POST",
            url: "/ui/tools/realwave/poll",
            data: {
                tenantId: miru.realwave.input.tenantId,
                startTimestamp: miru.realwave.input.startTimestamp,
                lookbackSeconds: miru.realwave.input.lookbackSeconds,
                buckets: miru.realwave.input.buckets,
                field1: miru.realwave.input.field1,
                terms1: miru.realwave.input.terms1,
                field2: miru.realwave.input.field2,
                terms2: miru.realwave.input.terms2,
                filters: miru.realwave.input.filters
            },
            //contentType: "application/json",
            success: function (data) {
                miru.realwave.draw(data);
            },
            error: function () {
                //TODO error message
                console.log("error!");
            }
        });
    },
    draw: function (data) {
        if (data.waveforms) {
            var i;
            if (!miru.realwave.chart) {
                var ctx = $('#rw-canvas')[0].getContext("2d");
                var chartData = {
                    labels: [],
                    datasets: []
                };
                var secsPerBucket = miru.realwave.input.lookbackSeconds / miru.realwave.input.buckets;
                for (i = 0; i < miru.realwave.input.buckets; i++) {
                    var secsAgo = secsPerBucket * (miru.realwave.input.buckets - i);
                    chartData.labels.push(miru.realwave.elapsed(Math.round(secsAgo)));
                }
                i = 0;
                $.each(data.waveforms, function (key, value) {
                    chartData.datasets.push({
                        label: key,
                        fillColor: miru.realwave.fillColors[i % miru.realwave.fillColors.length],
                        strokeColor: miru.realwave.strokeColors[i % miru.realwave.strokeColors.length],
                        highlightFill: miru.realwave.highlightFills[i % miru.realwave.highlightFills.length],
                        highlightStroke: miru.realwave.highlightStrokes[i % miru.realwave.highlightStrokes.length],
                        data: value
                    });
                    i++;
                });
                miru.realwave.chart = (new Chart(ctx))[miru.realwave.graphType](chartData, {
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
                $('#rw-legend').html(miru.realwave.chart.generateLegend());
            }
            //data.startBucketIndex;
            //data.elapse;
            i = 0;
            $.each(data.waveforms, function (key, value) {
                if (i < miru.realwave.chart.datasets.length) {
                    for (var j = 0; j < value.length; j++) {
                        miru.realwave.chart.datasets[i][miru.realwave.graphProp][j].value = value[j];
                    }
                }
                i++;
            });
            if (!miru.realwave.requireFocus || miru.windowFocused) {
                miru.realwave.chart.update();
            }
        }
        setTimeout(miru.realwave.poll, 1000);
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

$(document).ready(function () {

    if ($.fn.dropdown) {
        $('.dropdown-toggle').dropdown();
    }

    miru.windowFocused = true;
    miru.onWindowFocus = [];
    miru.onWindowBlur = [];

    if ($('#rw-waveform').length) {
        miru.realwave.init();
    }
});

$(window).focus(function () {
    miru.windowFocused = true;
    for (var i = 0; i < miru.onWindowFocus.length; i++) {
        miru.onWindowFocus[i]();
    }
}).blur(function () {
    miru.windowFocused = false;
    for (var i = 0; i < miru.onWindowBlur.length; i++) {
        miru.onWindowBlur[i]();
    }
});
