window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.balancer = {

    repair: function(ele) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/topology/repair",
            data: {},
            //contentType: "application/json",
            success: function() {
                $button.val('Success');
                setTimeout(function() {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function() {
                $button.val('Failure');
                setTimeout(function() {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    },

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
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function() {
                $button.val('Failure');
                setTimeout(function() {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    },

    remove: function(ele, host, port) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "DELETE",
            url: "/miru/manage/hosts/" + host + "/" + port,
            //contentType: "application/json",
            success: function() {
                $button.val('Success');
                setTimeout(function() {
                    miru.resetButton($button, value);
                }, 2000);
            },
            error: function() {
                $button.val('Failure');
                setTimeout(function() {
                    miru.resetButton($button, value);
                }, 2000);
            }
        });
    }
};

miru.tenants = {

    rebuild: function (ele, host, port, tenantId, partitionId) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/tenants/rebuild",
            data: {
                "host": host,
                "port": port,
                "tenantId": tenantId,
                "partitionId": partitionId
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

miru.activitywal = {

    repair: function (ele) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/miru/manage/wal/repair",
            data: {},
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
