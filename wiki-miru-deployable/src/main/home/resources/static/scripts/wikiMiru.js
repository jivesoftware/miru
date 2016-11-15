window.$ = window.jQuery;

window.wiki = {};


/*
<div>
    <input form="filter-form" type="text" autocomplete="off" name="cluster" id="clusterPicker"
        class="typeahead-field form-control" data-typeahead-url="/ui/lookup">
    <input form="filter-form" type="hidden" name="clusterKey" value="{$filters.clusterKey?:''}" />
</div>

*/
wiki.hs = {
    installed: null,
    queuedUninstall: null,
    init: function () {
        $('input[type=text].typeahead-field').each(function (i, input) {
            var $inputName = $(input);
            var $inputKey = $(input).next('input[type=hidden]');
            var endpoint = $inputName.data('typeaheadUrl');
            var ab = $inputName.data('typeaheadArgs');

            $inputName.focus(function () {
                wiki.hs.install($inputKey, $inputName, function (key, name) {
                    $inputKey.val(key);
                    $inputName.val(name);
                });
                wiki.hs.lookup(endpoint, ab, $inputName.val());
            });
            $inputName.on('input', function () {
                $inputKey.val('');
                wiki.hs.lookup(endpoint, ab, $inputName.val());
            });
            $inputName.blur(function () {
                wiki.hs.queuedUninstall = setTimeout(function () {
                    wiki.hs.uninstall($inputName);
                }, 200);
            });
        });
    },
    install: function ($inputKey, $inputName, callback) {
        wiki.hs.cancelUninstall();
        wiki.hs.uninstall();
        var $selector = wiki.hs.makeSelector();
        $('body').append($selector);
        wiki.hs.installed = {
            selector: $selector,
            inputKey: $inputKey,
            inputName: $inputName,
            callback: callback,
            ready: false
        };
        $inputName.removeClass('wiki-hs-field-broken');
        var offset = $inputName.offset();
        var height = $inputName.height();
        $selector.show();
        $selector.offset({
            left: offset.left,
            top: offset.top + height + 10
        });
    },
    uninstall: function ($inputName) {
        if (!wiki.hs.installed || $inputName && wiki.hs.installed.inputName != $inputName) {
            return;
        }

        $inputName = $inputName || wiki.hs.installed.inputName;
        var $inputKey = wiki.hs.installed.inputKey;
        var name = $inputName.val();
        var found = false;
        var $selector = wiki.hs.installed.selector;
        $selector.find('a').each(function (i) {
            var $a = $(this);
            if ($a.data('upenaName') == name) {
                var key = $a.data('upenaKey');
                $inputKey.val(key);
                found = true;
                return false;
            }
        });
        if (!found) {
            $inputName.addClass('wiki-hs-field-broken');
            wiki.hs.installed.inputKey.val('');
        }

        $selector.remove();
        wiki.hs.installed = null;
    },
    makeSelector: function () {
        var $selector = $('<div>').addClass("wiki-hs-selector");
        $selector.focus(function () {
            if (selector == wiki.hs.installed.selector) {
                wiki.hs.cancelUninstall();
            }
        });
        $selector.blur(function () {
            wiki.hs.uninstall();
        });
        return $selector;
    },
    cancelUninstall: function () {
        if (wiki.hs.queuedUninstall) {
            clearTimeout(wiki.hs.queuedUninstall);
        }
    },
    picked: function (key, name) {
        if (wiki.hs.installed) {
            wiki.hs.installed.callback(key, name);
            wiki.hs.uninstall();
        }
    },
    lookup: function (endpoint, ab, contains) {


        var host = "#" + ab + "RemoteHostPicker";
        var port = "#" + ab + "RemotePortPicker";

        console.log(host + " " + port);

        console.log($(host).attr('value') + " " + $(port).attr('value'));

        var $selector = wiki.hs.installed.selector;
        $.ajax(endpoint, {
            data: {
                'contains': contains,
                'remoteHost': $(host).attr('value'),
                'remotePort': $(port).attr('value')
            }
        })
                .done(function (data) {
                    if (!wiki.hs.installed || wiki.hs.installed.selector != $selector) {
                        // selector changed during the query
                        return;
                    }
                    if (data.length) {
                        $selector.empty();
                        for (var i = 0; i < data.length; i++) {
                            $selector.append(
                                    "<a href='#'" +
                                    " class='wiki-hs-choice'" +
                                    " data-wiki-key='" + data[i].key + "'" +
                                    " data-wiki-name='" + data[i].name + "'>" + data[i].name + "</a><br/>");
                        }
                        wiki.hs.link($selector);
                        wiki.hs.installed.ready = true;
                    } else {
                        $selector.html("<em>No matches</em>");
                    }
                });
    },
    link: function ($selector) {
        $selector.find('a').each(function (i) {
            var $a = $(this);
            var key = $a.data('upenaKey');
            var name = $a.data('upenaName');
            $a.click(function () {
                wiki.hs.picked(key, name);
                return false;
            });
        });
    }
};


$(document).ready(function () {
    wiki.windowFocused = true;
    wiki.onWindowFocus = [];
    wiki.onWindowBlur = [];

    if ($('.typeahead-field').length) {
        wiki.hs.init();
    }

});


$(window).focus(function () {
    wiki.windowFocused = true;
    for (var i = 0; i < wiki.onWindowFocus.length; i++) {
        wiki.onWindowFocus[i]();
    }
}).blur(function () {
    wiki.windowFocused = false;
    for (var i = 0; i < wiki.onWindowBlur.length; i++) {
        wiki.onWindowBlur[i]();
    }
});
