window.$ = window.jQuery;

window.siphon = {};


/*
<div>
    <input form="filter-form" type="text" autocomplete="off" name="cluster" id="clusterPicker"
        class="typeahead-field form-control" data-typeahead-url="/ui/lookup">
    <input form="filter-form" type="hidden" name="clusterKey" value="{$filters.clusterKey?:''}" />
</div>

*/
siphon.hs = {
    installed: null,
    queuedUninstall: null,
    init: function () {
        $('input[type=text].typeahead-field').each(function (i, input) {
            var $inputName = $(input);
            var $inputKey = $(input);
            var endpoint = $inputName.data('typeaheadUrl');
            var tenant = $($inputName.data('typeaheadTenantId')).val();
            endpoint = endpoint+tenant;

            $inputName.focus(function () {
                siphon.hs.install($inputKey, $inputName, function (key, name) {
                    $inputKey.val(key);
                    $inputName.val(name);
                });
                siphon.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.on('input', function () {
                //$inputKey.val('');
                siphon.hs.lookup(endpoint, $inputName.val());
            });
            $inputName.blur(function () {
                siphon.hs.queuedUninstall = setTimeout(function () {
                    siphon.hs.uninstall($inputName);
                }, 200);
            });
        });
    },
    install: function ($inputKey, $inputName, callback) {
        siphon.hs.cancelUninstall();
        siphon.hs.uninstall();
        var $selector = siphon.hs.makeSelector();
        $('body').append($selector);
        siphon.hs.installed = {
            selector: $selector,
            inputKey: $inputKey,
            inputName: $inputName,
            callback: callback,
            ready: false
        };
        $inputName.removeClass('siphon-hs-field-broken');
        var offset = $inputName.offset();
        var height = $inputName.height();
        $selector.show();
        $selector.offset({
            left: offset.left,
            top: offset.top + height + 10
        });
    },
    uninstall: function ($inputName) {
        if (!siphon.hs.installed || $inputName && siphon.hs.installed.inputName != $inputName) {
            return;
        }

        $inputName = $inputName || siphon.hs.installed.inputName;
        var $inputKey = siphon.hs.installed.inputKey;
        var name = $inputName.val();
        var found = false;
        var $selector = siphon.hs.installed.selector;
        $selector.find('a').each(function (i) {
            var $a = $(this);
            if ($a.data('siphonName') == name) {
                var key = $a.data('siphonKey');
                $inputKey.val(key);
                found = true;
                return false;
            }
        });
        if (!found) {
            $inputName.addClass('siphon-hs-field-broken');
            siphon.hs.installed.inputKey.val('');
        }

        $selector.remove();
        siphon.hs.installed = null;
    },
    makeSelector: function () {
        var $selector = $('<div>').addClass("siphon-hs-selector");
        $selector.focus(function () {
            if (selector == siphon.hs.installed.selector) {
                siphon.hs.cancelUninstall();
            }
        });
        $selector.blur(function () {
            siphon.hs.uninstall();
        });
        return $selector;
    },
    cancelUninstall: function () {
        if (siphon.hs.queuedUninstall) {
            clearTimeout(siphon.hs.queuedUninstall);
        }
    },
    picked: function (key, name) {
        if (siphon.hs.installed) {
            siphon.hs.installed.callback(key, name);
            siphon.hs.uninstall();
        }
    },
    lookup: function (endpoint, contains) {

        var $selector = siphon.hs.installed.selector;
        $.ajax(endpoint, {
            data: {
                'contains': contains
            }
        }).done(function (data) {
            if (!siphon.hs.installed || siphon.hs.installed.selector != $selector) {
                // selector changed during the query
                return;
            }
            if (data.length) {
                $selector.empty();
                for (var i = 0; i < data.length; i++) {
                    $selector.append(
                            "<a href='#'" +
                            " class='siphon-hs-choice'" +
                            " data-siphon-key='" + data[i].key + "'" +
                            " data-siphon-name='" + data[i].name + "'>" + data[i].name + "</a><br/>");
                }
                siphon.hs.link($selector);
                siphon.hs.installed.ready = true;
            } else {
                $selector.html("<em>No matches</em>");
            }
        });
    },
    link: function ($selector) {
        $selector.find('a').each(function (i) {
            var $a = $(this);
            var key = $a.data('siphonKey');
            var name = $a.data('siphonName');
            $a.click(function () {
                siphon.hs.picked(key, name);
                return false;
            });
        });
    }
};


$(document).ready(function () {
    siphon.windowFocused = true;
    siphon.onWindowFocus = [];
    siphon.onWindowBlur = [];

    if ($('.typeahead-field').length) {
        siphon.hs.init();
    }

});


$(window).focus(function () {
    siphon.windowFocused = true;
    for (var i = 0; i < siphon.onWindowFocus.length; i++) {
        siphon.onWindowFocus[i]();
    }
}).blur(function () {
    siphon.windowFocused = false;
    for (var i = 0; i < siphon.onWindowBlur.length; i++) {
        siphon.onWindowBlur[i]();
    }
});
