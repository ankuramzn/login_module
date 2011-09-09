// Place your application-specific JavaScript functions and classes here
// This file is automatically included by javascript_include_tag :defaults

$(document).ready(function() {
    $('#json_link').click(function(){
        $.ajax({
           url: '/posts/index.json',
           dataType: 'json',
           success: function(data){
               $.each(data,function(i,post_data){
                   console.log("Iterated Object " + post_data.post.title);
               });
           }
        });
    });
});