window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.catwalk = {
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

    if ($('#catwalk').length) {
        miru.catwalk.init();
    }
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
