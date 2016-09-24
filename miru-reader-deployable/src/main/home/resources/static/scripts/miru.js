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
            var data = miru.lab.data[id];
            
            miru.lab.waves[id] = new Chart(ctx, {
                type: 'line',
                data: data,
                options: {
                    maintainAspectRatio: false,
                    responsive: true,
                    legend: {
                        display: false
                    },
                    gridLines: {
                        color: "rgba(128,128,128,1)"
                    },
                    scaleLabel: {
                        fontColor: "rgba(200,200,200,1)"
                    },
                    scales: {
                        yAxes: [{
                            position: "right",
                            ticks: {
                                beginAtZero:true
                            }
                        }]
                    }//,
//                    legendCallback: function(chart) {
//                        //console.log(chart.data);
//                        var text = [];
//                        text.push('<ul>');
//                        for (var i=0; i<chart.data.datasets[0].data.length; i++) {
//                            text.push('<li>');
//                            text.push('<span style="background-color:' + chart.data.datasets[0].backgroundColor[i] + '">' + chart.data.datasets[0].data[i] + '</span>');
//                            if (chart.data.labels[i]) {
//                                text.push(chart.data.labels[i]);
//                            }
//                            text.push('</li>');
//                        }
//                        text.push('</ul>');
//                        return text.join("");
//                    }
                }
            });
        }
        //miru.lab.waves[id].generateLegend();
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
