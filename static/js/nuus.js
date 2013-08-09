$(function() {
    $('a[data-nzbname]').click(function() {
        var name = $(this).data('name');
        var nzbname = $(this).data('nzbname');
        console.log(name, nzbname);
        url = sabnzbd.host + '/api%apikey=' + sabnzbd.apikey + '&name=' + name + '&nzbname=' + nzbname;
        $.ajax(url,{
            parseData: false,
            success: function() {
                $.bootstrapGrowl('Successfully added ' + name, {
                    type: 'success', delay: 4000, align: 'right', offset: {from: 'bottom', amount: 0}});
            },
            error: function(o, e) {
                console.log(e);
                $.bootstrapGrowl('Failed to add ' + name, {
                    type: 'danger', align: 'right', offset: {from: 'bottom', amount: 0}});
            }
        });
    });
});
