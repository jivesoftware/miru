window.$ = window.jQuery;

window.anomaly = {};

anomaly.query = {

    waves: {},
    data: {},

    initChart: function (which) {
        var $canvas = $(which);
        var ctx = which.getContext("2d");
        var id = $canvas.data('anomWaveId');
        if (!anomaly.query.waves[id]) {
            var type = $canvas.data('anomWaveType');
            var data = anomaly.query.data[id];
            anomaly.query.waves[id] = (new Chart(ctx))[type](data, {
                multiTooltipTemplate: "<%= datasetLabel %> - <%= value %>",
                scaleLineColor: "rgba(128,128,128,0.5)",
                tooltipFillColor: "rgba(0,0,0,1)",
                pointDot: false,
                bezierCurve: false,
                datasetFill: false,
                responsive: true,
                animation: false
            });
        }
        anomaly.query.waves[id].update();
    },

    init: function () {
        anomaly.query.initToggle();

        $('.anom-single-wave').each(function (i) {
            anomaly.query.initChart(this);
        });

        var isLarge = [];
        $('.anom-wave-toggle').each(function (i) {
            $(this).click(function () {
                $(this).animate({height: (isLarge[i] ? '36px' : '100%')});
                isLarge[i] = !isLarge[i];
            });
        });

        $('#tabs a[data-toggle=tab]').on('shown.bs.tab', function (e) {
            var $a = $(this);
            var selector = $a.attr('href');
            $(selector).find('canvas').each(function (i) {
                anomaly.query.initChart(this);
            });
        });
        $('#tabs a[data-toggle=tab]:first').tab('show');
    },

    advanced: function (ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#anom-query-filters').addClass('anom-query-show-advanced');
        } else {
            $('#anom-query-filters').removeClass('anom-query-show-advanced');
        }
    },

    toggle: function (ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#anom-events').addClass('anom-show-' + $e.data('name'));
        } else {
            $('#anom-events').removeClass('anom-show-' + $e.data('name'));
        }
    },

    initToggle: function () {
        var $toggle = $('.anom-toggle');
        var $toggleOn = $('.anom-toggle-on');

        $toggle.prop('checked', false);
        $toggleOn.prop('checked', true);
        $toggle.each(function (index, ele) {
            anom.query.toggle(ele);
        });
    }
};

$(document).ready(function () {
    if ($('#anomaly-query').length) {
        anomaly.query.init();
    }
});
