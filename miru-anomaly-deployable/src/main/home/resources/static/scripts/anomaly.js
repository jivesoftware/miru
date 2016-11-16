window.$ = window.jQuery;

window.anomaly = {};

/*
<div>
    <input form="filter-form" type="text" autocomplete="off" name="cluster" id="clusterPicker"
        class="typeahead-field form-control" data-typeahead-url="/ui/lookup">
    <input form="filter-form" type="hidden" name="clusterKey" value="{$filters.clusterKey?:''}" />
</div>

*/
anomaly.hs = {
    installed: null,
    queuedUninstall: null,
    init: function () {
        $('input[type=text].typeahead-field').each(function (i, input) {
            var $inputName = $(input);
            var $inputKey = $(input);
            var endpoint = $inputName.data('typeaheadUrl');

            $inputName.focus(function () {
                anomaly.hs.install($inputKey, $inputName, function (key, name) {
                    $inputKey.val(key);
                    $inputName.val(name);
                });
                anomaly.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.on('input', function () {
                $inputKey.val('');
                anomaly.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.blur(function () {
                anomaly.hs.queuedUninstall = setTimeout(function () {
                    anomaly.hs.uninstall($inputName);
                }, 200);
            });
        });
    },
    install: function ($inputKey, $inputName, callback) {
        anomaly.hs.cancelUninstall();
        anomaly.hs.uninstall();
        var $selector = anomaly.hs.makeSelector();
        $('body').append($selector);
        anomaly.hs.installed = {
            selector: $selector,
            inputKey: $inputKey,
            inputName: $inputName,
            callback: callback,
            ready: false
        };
        $inputName.removeClass('anomaly-hs-field-broken');
        var offset = $inputName.offset();
        var height = $inputName.height();
        $selector.show();
        $selector.offset({
            left: offset.left,
            top: offset.top + height + 10
        });
    },
    uninstall: function ($inputName) {
        if (!anomaly.hs.installed || $inputName && anomaly.hs.installed.inputName != $inputName) {
            return;
        }

        $inputName = $inputName || anomaly.hs.installed.inputName;
        var $inputKey = anomaly.hs.installed.inputKey;
        var name = $inputName.val();
        var found = false;
        var $selector = anomaly.hs.installed.selector;
        $selector.find('a').each(function (i) {
            var $a = $(this);
            if ($a.data('anomalyName') == name) {
                var key = $a.data('anomalyKey');
                $inputKey.val(key);
                found = true;
                return false;
            }
        });
        if (!found) {
            $inputName.addClass('anomaly-hs-field-broken');
            anomaly.hs.installed.inputKey.val('');
        }

        $selector.remove();
        anomaly.hs.installed = null;
    },
    makeSelector: function () {
        var $selector = $('<div>').addClass("anomaly-hs-selector");
        $selector.focus(function () {
            if (selector == anomaly.hs.installed.selector) {
                anomaly.hs.cancelUninstall();
            }
        });
        $selector.blur(function () {
            anomaly.hs.uninstall();
        });
        return $selector;
    },
    cancelUninstall: function () {
        if (anomaly.hs.queuedUninstall) {
            clearTimeout(anomaly.hs.queuedUninstall);
        }
    },
    picked: function (key, name) {
        if (anomaly.hs.installed) {
            anomaly.hs.installed.callback(key, name);
            anomaly.hs.uninstall();
        }
    },
    lookup: function (endpoint, contains) {

        var $selector = anomaly.hs.installed.selector;
        $.ajax(endpoint, {
            data: {
                'contains': contains
            }
        }).done(function (data) {
            if (!anomaly.hs.installed || anomaly.hs.installed.selector != $selector) {
                // selector changed during the query
                return;
            }
            if (data.length) {
                $selector.empty();
                for (var i = 0; i < data.length; i++) {
                    $selector.append(
                            "<a href='#'" +
                            " class='anomaly-hs-choice'" +
                            " data-anomaly-key='" + data[i].key + "'" +
                            " data-anomaly-name='" + data[i].name + "'>" + data[i].name + "</a><br/>");
                }
                anomaly.hs.link($selector);
                anomaly.hs.installed.ready = true;
            } else {
                $selector.html("<em>No matches</em>");
            }
        });
    },
    link: function ($selector) {
        $selector.find('a').each(function (i) {
            var $a = $(this);
            var key = $a.data('anomalyKey');
            var name = $a.data('anomalyName');
            $a.click(function () {
                anomaly.hs.picked(key, name);
                return false;
            });
        });
    }
};

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
                datasetFill: true,
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
    },

    focus: function (field, value) {
        $('#' + field + 'Picker').val(value);
        $('#anom-query-filters').submit();
    }
};

$(document).ready(function () {


    if ($('.typeahead-field').length) {
        anomaly.hs.init();
    }


    if ($('#anomaly-query').length) {
        anomaly.query.init();
    }
});
