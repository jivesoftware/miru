window.$ = window.jQuery;

window.stump = {};

stump.query = {

    advanced: function(ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-query-filters').addClass('stump-query-show-advanced');
        } else {
            $('#stump-query-filters').removeClass('stump-query-show-advanced');
        }
    },

    toggle: function(ele) {
        var $e = $(ele);
        if ($e.prop('checked')) {
            $('#stump-events').addClass('stump-show-' + $e.data('name'));
        } else {
            $('#stump-events').removeClass('stump-show-' + $e.data('name'));
        }
    },

    initEvents: function() {
        var $toggle = $('.stump-toggle');
        var $toggleOn = $('.stump-toggle-on');

        $toggle.prop('checked', false);
        $toggleOn.prop('checked', true);
        $toggle.each(function(index, ele) {
            stump.query.toggle(ele);
        });
    }
};

$(document).ready(function() {
    stump.query.initEvents();
});