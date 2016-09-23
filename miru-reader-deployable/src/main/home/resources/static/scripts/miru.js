window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.partitions = {
    rebuild: function (ele) {
        var days = $('#days').val();
        var hotDeploy = $('#hotDeploy').is(':checked');
        var chunkStores = $('#chunkStores').is(':checked');
        var labIndexes = $('#labIndexes').is(':checked');

        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/config/rebuild",
            data: {
                "days": days,
                "hotDeploy": hotDeploy,
                "chunkStores": chunkStores,
                "labIndexes": labIndexes
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

miru.errors = {
    rebuild: function (ele, tenantId, partitionId) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/config/rebuild/prioritize/" + tenantId + "/" + partitionId,
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


miru.lab = {
    waves: {},
    data: {},
    initChart: function (which) {
        var $canvas = $(which);
        var ctx = which.getContext("2d");
        var id = $canvas.data('labWaveId');
        if (!miru.lab.waves[id]) {
            var type = $canvas.data('labWaveType');
            var data = miru.lab.data[id];
            miru.lab.waves[id] = (new Chart(ctx))[type](data, {
                multiTooltipTemplate: "<%= datasetLabel %> - <%= value %>",
                scaleLineColor: "rgba(128,128,128,0.5)",
                tooltipFillColor: "rgba(0,0,0,1)",
                pointDot: false,
                steppedLine: true,
                pointDotRadius: 0,
                datasetFill: false,
                responsive: true,
                animation: false,
                legendTemplate: "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<datasets.length; i++){%><li><span style=\"background-color:<%=datasets[i].strokeColor%>\"></span><%if(datasets[i].label){%><%=datasets[i].label%><%}%></li><%}%></ul>"
            });

            miru.lab.waves[id].generateLegend();
        }
        miru.lab.waves[id].update();
    },
    init: function () {

        $('.lab-wave').each(function (i) {
            miru.lab.initChart(this);
        });
    }
};

$(document).ready(function () {
     if ($('.lab-wave').length) {
        miru.lab.init();
    }

    if ($('.lab-scroll-wave').length) {

        $('.lab-scroll-wave').each(function (j, va) {
            $(va).on('scroll', function () {
                $('.lab-scroll-wave').each(function (j, vb) {
                    if ($(va) !== $(vb)) {
                        $(vb).scrollLeft($(va).scrollLeft());
                    }
                });
            });
        });
    }
});
