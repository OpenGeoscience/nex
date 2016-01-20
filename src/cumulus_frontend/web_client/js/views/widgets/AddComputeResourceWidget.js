minerva.views.AddComputeResourceWidget = minerva.View.extend({
    initialize: function(settings) {
        this.collection = settings.collection;
    },

    render: function () {
        var view = this;
        var modal = this.$el.html(girder.templates.addComputeResourceWidget({

        })).girderModal(this).on('ready.girder.modal', _.bind(function(){
            alert("foo!");
        }, this));


    }
})
