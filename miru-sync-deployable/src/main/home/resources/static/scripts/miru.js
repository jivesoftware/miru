window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.statusFocus = {
    init: function () {
    },

    reset: function(ele, syncspaceName, tenant) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/sync/reset/" + syncspaceName + "/" + tenant,
            data: {},
            //contentType: "application/json",
            success: function () {
                window.location.reload(true);
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

$(document).ready(function () {
    if ($.fn.dropdown) {
        $('.dropdown-toggle').dropdown();
    }

    miru.windowFocused = true;
    miru.onWindowFocus = [];
    miru.onWindowBlur = [];

    if ($('#status-focus').length) {
        miru.statusFocus.init();
    }

    $(function () {
        var hack = {};
        $('[rel="popover"]').popover({
            container: 'body',
            html: true,
            content: function () {
                console.log($(this).attr('id'));
                var h = $($(this).data('popover-content')).removeClass('hide');
                hack[$(this).attr('id')] = h;
                return h;
            }
        }).click(function (e) {
            e.preventDefault();
        }).on('hidden.bs.popover', function () {
            var h = hack[$(this).attr('id')];
            h.detach();
            h.addClass('hide');
            $('body').append(h);
        });
    });
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
