minerva.views.SelectAWSProfile = minerva.View.extend({
    initialize: function(settings){
        this.profiles = settings.profiles;
    },

    render: function(){
        this.$el.html(girder.templates.selectAWSProfile({
            profiles: this.profiles
        }));
    }
})
