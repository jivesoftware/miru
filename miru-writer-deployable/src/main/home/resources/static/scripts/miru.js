window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.writer = {
    init: function () {
    }
};

$(document).ready(function () {
    if ($.fn.dropdown) {
        $('.dropdown-toggle').dropdown();
    }

    miru.windowFocused = true;
    miru.onWindowFocus = [];
    miru.onWindowBlur = [];

    if ($('#writer').length) {
        miru.writer.init();
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
