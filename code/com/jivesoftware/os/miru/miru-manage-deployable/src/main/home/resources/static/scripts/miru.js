window.$ = window.jQuery;

window.miru = {};

miru.balancer = {
    rebalance: function(ele, host, port, direction) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/topology/shift",
            data: {
                "host": host,
                "port": port,
                "direction": direction
            },
            //contentType: "application/json",
            success: function() {
                $button.val('Success');
                setTimeout(function() {
                    miru.balancer.resetButton($button, value);
                }, 2000);
            },
            error: function() {
                $button.val('Failure');
                setTimeout(function() {
                    miru.balancer.resetButton($button, value);
                }, 2000);
            }
        });
    },

    resetButton: function($button, value) {
        $button.val(value);
        $button.removeAttr('disabled');
    }
};
