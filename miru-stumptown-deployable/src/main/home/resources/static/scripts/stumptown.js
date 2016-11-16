window.$ = window.jQuery;

window.stump = {};

/*
<div>
    <input form="filter-form" type="text" autocomplete="off" name="cluster" id="clusterPicker"
        class="typeahead-field form-control" data-typeahead-url="/ui/lookup">
    <input form="filter-form" type="hidden" name="clusterKey" value="{$filters.clusterKey?:''}" />
</div>

*/
stump.hs = {
    installed: null,
    queuedUninstall: null,
    init: function () {
        $('input[type=text].typeahead-field').each(function (i, input) {
            var $inputName = $(input);
            var $inputKey = $(input);
            var endpoint = $inputName.data('typeaheadUrl');

            $inputName.focus(function () {
                stump.hs.install($inputKey, $inputName, function (key, name) {
                    $inputKey.val(key);
                    $inputName.val(name);
                });
                stump.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.on('input', function () {
                $inputKey.val('');
                stump.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.blur(function () {
                stump.hs.queuedUninstall = setTimeout(function () {
                    stump.hs.uninstall($inputName);
                }, 200);
            });
        });
    },
    install: function ($inputKey, $inputName, callback) {
        stump.hs.cancelUninstall();
        stump.hs.uninstall();
        var $selector = stump.hs.makeSelector();
        $('body').append($selector);
        stump.hs.installed = {
            selector: $selector,
            inputKey: $inputKey,
            inputName: $inputName,
            callback: callback,
            ready: false
        };
        $inputName.removeClass('stump-hs-field-broken');
        var offset = $inputName.offset();
        var height = $inputName.height();
        $selector.show();
        $selector.offset({
            left: offset.left,
            top: offset.top + height + 10
        });
    },
    uninstall: function ($inputName) {
        if (!stump.hs.installed || $inputName && stump.hs.installed.inputName != $inputName) {
            return;
        }

        $inputName = $inputName || stump.hs.installed.inputName;
        var $inputKey = stump.hs.installed.inputKey;
        var name = $inputName.val();
        var found = false;
        var $selector = stump.hs.installed.selector;
        $selector.find('a').each(function (i) {
            var $a = $(this);
            if ($a.data('stumpName') == name) {
                var key = $a.data('stumpKey');
                $inputKey.val(key);
                found = true;
                return false;
            }
        });
        if (!found) {
            $inputName.addClass('stump-hs-field-broken');
            stump.hs.installed.inputKey.val('');
        }

        $selector.remove();
        stump.hs.installed = null;
    },
    makeSelector: function () {
        var $selector = $('<div>').addClass("stump-hs-selector");
        $selector.focus(function () {
            if (selector == stump.hs.installed.selector) {
                stump.hs.cancelUninstall();
            }
        });
        $selector.blur(function () {
            stump.hs.uninstall();
        });
        return $selector;
    },
    cancelUninstall: function () {
        if (stump.hs.queuedUninstall) {
            clearTimeout(stump.hs.queuedUninstall);
        }
    },
    picked: function (key, name) {
        if (stump.hs.installed) {
            stump.hs.installed.callback(key, name);
            stump.hs.uninstall();
        }
    },
    lookup: function (endpoint, contains) {

        var $selector = stump.hs.installed.selector;
        $.ajax(endpoint, {
            data: {
                'contains': contains,
                'fromAgo': $(#fromAgoPicker).val(),
                'toAgo': $(#toAgoPicker).val(),
                'fromTimeUnit': $(#fromTimeUnitPicker).val(),
                'toTimeUnit': $(#toTimeUnitPicker).val(),

                'cluster': $(#clusterPicker).val(),
                'host': $(#hostPicker).val(),
                'service': $(#servicePicker).val(),
                'instance': $(#instancePicker).val(),
                'version': $(#versionPicker).val(),
                'method': $(#methodPicker).val(),
                'line': $(#linePicker).val(),
                'thread': $(#threadPicker).val(),
                'logger': $(#loggerPicker).val()
            }
        }).done(function (data) {
            if (!stump.hs.installed || stump.hs.installed.selector != $selector) {
                // selector changed during the query
                return;
            }
            if (data.length) {
                $selector.empty();
                for (var i = 0; i < data.length; i++) {
                    $selector.append(
                            "<a href='#'" +
                            " class='stump-hs-choice'" +
                            " data-stump-key='" + data[i].key + "'" +
                            " data-stump-name='" + data[i].name + "'>" + data[i].name + "</a><br/>");
                }
                stump.hs.link($selector);
                stump.hs.installed.ready = true;
            } else {
                $selector.html("<em>No matches</em>");
            }
        });
    },
    link: function ($selector) {
        $selector.find('a').each(function (i) {
            var $a = $(this);
            var key = $a.data('stumpKey');
            var name = $a.data('stumpName');
            $a.click(function () {
                stump.hs.picked(key, name);
                return false;
            });
        });
    }
};

stump.query = {
    advanced: function (ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-query-filters').addClass('stump-query-show-advanced');
        } else {
            $('#stump-query-filters').removeClass('stump-query-show-advanced');
        }
    },
    toggle: function (ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-rt-events').addClass('stump-show-' + $e.data('name'));
        } else {
            $('#stump-rt-events').removeClass('stump-show-' + $e.data('name'));
        }
    },
    initEvents: function () {
        var $toggle = $('.stump-toggle');
        var $toggleOn = $('.stump-toggle-on');

        $toggle.prop('checked', false);
        $toggleOn.prop('checked', true);
        $toggle.each(function (index, ele) {
            stump.query.toggle(ele);
        });

        stump.realtime.init();
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

        stump.realtime.input.cluster = $waveform.data('cluster');
        stump.realtime.input.host = $waveform.data('host');
        stump.realtime.input.version = $waveform.data('version');
        stump.realtime.input.service = $waveform.data('service');
        stump.realtime.input.instance = $waveform.data('instance');
        stump.realtime.input.logLevels = $waveform.data('logLevels');
        stump.realtime.input.fromAgo = $waveform.data('fromAgo');
        stump.realtime.input.toAgo = $waveform.data('toAgo');
        stump.realtime.input.fromTimeUnit = $waveform.data('fromTimeUnit');
        stump.realtime.input.toTimeUnit = $waveform.data('toTimeUnit');
        stump.realtime.input.thread = $waveform.data('thread');
        stump.realtime.input.logger = $waveform.data('logger');
        stump.realtime.input.method = $waveform.data('method');
        stump.realtime.input.line = $waveform.data('line');
        stump.realtime.input.message = $waveform.data('message');
        stump.realtime.input.exceptionClass = $waveform.data('exceptionClass');
        stump.realtime.input.thrown = $waveform.data('thrown');
        stump.realtime.input.events = $waveform.data('events');
        stump.realtime.input.buckets = $waveform.data('buckets');
        stump.realtime.input.messageCount = $waveform.data('messageCount');

        stump.realtime.requireFocus = $waveform.data('requireFocus') != "false";
        stump.realtime.graphType = $waveform.data('graphType');
        stump.realtime.graphProp = (stump.realtime.graphType == 'Line' || stump.realtime.graphType == 'Radar') ? 'points'
                : (stump.realtime.graphType == 'Bar' || stump.realtime.graphType == 'StackedBar') ? 'bars'
                : 'unknown';

        if (stump.realtime.requireFocus) {
            stump.onWindowFocus.push(function () {
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
            url: "/ui/query/poll",
            data: {
                cluster: stump.realtime.input.cluster,
                host: stump.realtime.input.host,
                version: stump.realtime.input.version,
                service: stump.realtime.input.service,
                instance: stump.realtime.input.instance,
                logLevels: stump.realtime.input.logLevels,
                fromAgo: stump.realtime.input.fromAgo,
                toAgo: stump.realtime.input.toAgo,
                fromTimeUnit: stump.realtime.input.fromTimeUnit,
                toTimeUnit: stump.realtime.input.toTimeUnit,
                thread: stump.realtime.input.thread,
                logger: stump.realtime.input.logger,
                method: stump.realtime.input.method,
                line: stump.realtime.input.line,
                message: stump.realtime.input.message,
                exceptionClass: stump.realtime.input.exceptionClass,
                thrown: stump.realtime.input.thrown,
                events: stump.realtime.input.events,
                buckets: stump.realtime.input.buckets,
                messageCount: stump.realtime.input.messageCount
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
                var rangeInSecs = data.fromAgoSecs - data.toAgoSecs;
                var secsPerBucket = rangeInSecs / stump.realtime.input.buckets;
                for (i = 0; i < stump.realtime.input.buckets; i++) {
                    var secsAgo = data.toAgoSecs + secsPerBucket * (stump.realtime.input.buckets - i);
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
            if (!stump.realtime.requireFocus || stump.windowFocused) {
                stump.realtime.chart.update();
            }
        }
        if (data.logEvents) {
            $('tr.stump-rt-log-event').remove();
            for (i = 0; i < data.logEvents.length; i++) {
                stump.realtime.eventsBody.append(data.logEvents[i]);
            }
        }
        if ($('#live').prop("checked")) {
            setTimeout(stump.realtime.poll, 1000);
        }
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
    stump.windowFocused = true;
    stump.onWindowFocus = [];
    stump.onWindowBlur = [];

    if ($('.typeahead-field').length) {
            stump.hs.init();
        }

    $('#live').change(function () {
        if ($(this).is(":checked")) {
            stump.realtime.poll();
        }
    });

    stump.query.initEvents();
});

$(window).focus(function () {
    stump.windowFocused = true;
    for (var i = 0; i < stump.onWindowFocus.length; i++) {
        stump.onWindowFocus[i]();
    }
}).blur(function () {
    stump.windowFocused = false;
    for (var i = 0; i < stump.onWindowBlur.length; i++) {
        stump.onWindowBlur[i]();
    }
});
