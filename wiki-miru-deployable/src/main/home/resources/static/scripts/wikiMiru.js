window.$ = window.jQuery;

window.wiki = {};

$(document).ready(function () {
    wiki.windowFocused = true;
    wiki.onWindowFocus = [];
    wiki.onWindowBlur = [];


    $('.typeahead').typeahead({
       source: function (query, process) {
           var url = '/ui/query/typeahead/'+$('#typeahead-tenantId').val()+'/'+query;
           console.log(url);
           return $.get(url, { }, function (data) {
                console.log(data);
               return process(data);
           });
       },
       showHintOnFocus:'all'
   });
});

$(document).onkeydown = function(evt) {
    evt = evt || window.event;
    if (evt.ctrlKey && evt.keyCode == 90) {
        alert("Ctrl-Z");
    }
};

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
