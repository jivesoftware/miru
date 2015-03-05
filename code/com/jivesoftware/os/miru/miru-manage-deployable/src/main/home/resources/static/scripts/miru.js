window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.balancer = {

    repair: function (ele) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/topology/repair",
            data: {},
            //contentType: "application/json",
            success: function () {
                $button.val('Success');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    },

    rebalance: function (ele, host, port, direction) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/topology/shift",
            data: {
                "host": host,
                "port": port,
                "direction": direction
            },
            //contentType: "application/json",
            success: function () {
                $button.val('Success');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    },

    remove: function (ele, host, port) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "DELETE",
            url: "/miru/manage/hosts/" + host + "/" + port,
            //contentType: "application/json",
            success: function () {
                $button.val('Success');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    }
};

miru.tenants = {

    rebuild: function (ele, host, port, tenantId, partitionId) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/tenants/rebuild",
            data: {
                "host": host,
                "port": port,
                "tenantId": tenantId,
                "partitionId": partitionId
            },
            //contentType: "application/json",
            success: function () {
                $button.val('Success');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    }
};

miru.activitywal = {

    repair: function (ele) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/wal/repair",
            data: {},
            //contentType: "application/json",
            success: function () {
                $button.val('Success');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    }
};

miru.realwave = {

    input: {},
    lastBucketIndex: -1,
    chart: null,
    labels: [],

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

        var secsPerBucket = miru.realwave.input.lookbackSeconds / miru.realwave.input.buckets;
        for (var i = 0; i < miru.realwave.input.buckets; i++) {
            var secsAgo = secsPerBucket * (miru.realwave.input.buckets - i);
            miru.realwave.labels.push(Math.round(secsAgo) + "s");
        }

        miru.realwave.poll();
    },

    fillColors: [
        "rgba(220,220,220,0.5)",
        "rgba(151,187,205,0.5)"
    ],
    strokeColors: [
        "rgba(220,220,220,0.8)",
        "rgba(151,187,205,0.8)"
    ],
    highlightFills: [
        "rgba(220,220,220,0.75)",
        "rgba(151,187,205,0.75)"
    ],
    highlightStrokes: [
        "rgba(220,220,220,1)",
        "rgba(151,187,205,1)"
    ],

    poll: function () {
        $.ajax({
            type: "POST",
            url: "/miru/manage/realwave/poll",
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
                var ctx = $('#waveforms')[0].getContext("2d");
                var chartData = {
                    labels: miru.realwave.labels,
                    datasets: []
                };
                var empty = [];
                for (i = 0; i < miru.realwave.input.buckets; i++) {
                    var secsAgo = secsPerBucket * (miru.realwave.input.buckets - i);
                    miru.realwave.labels.push(Math.round(secsAgo) + "s");
                    empty.push(0);
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
                miru.realwave.chart = new Chart(ctx).StackedBar(chartData, {
                    multiTooltipTemplate: "<%= datasetLabel %> - <%= value %>",
                    scaleLineColor: "rgba(128,128,128,0.5)",
                    tooltipFillColor: "rgba(0,0,0,1)",
                    pointDot: false,
                    bezierCurve: false,
                    datasetFill: false,
                    responsive: true,
                    animation: true
                });
            }
            //data.startBucketIndex;
            //data.elapse;
            i = 0;
            $.each(data.waveforms, function (key, value) {
                for (var j = 0; j < value.length; j++) {
                    miru.realwave.chart.datasets[i].bars[j].value = value[j];
                }
                i++;
            });
            miru.realwave.chart.update();
        }
        setTimeout(miru.realwave.poll, 1000);
    }
};

$(document).ready(function () {
    if ($('#rw-waveform').length) {
        miru.realwave.init();
    }
});
