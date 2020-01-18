// dataset_summary.js
// code for summary tab of dataset.html detail page

$(function(){
  // TODO: reload should NOT be necessary
  console.log('dataset id:'+"{{ object.id }}")
  updateTotals('{{ object.id }}')
  steps={"uploaded":1,"reconciling":2,"review_hits":3,"reviewed":4,"review_whg":5,"indexed":6}
  $("[ref="+steps['{{ object.status }}']+"]").addClass('prog-active')

  $("#collabs_list a").click(function(){
    console.log($(this).data('uid'))
  })   
  $(".help-matches").click(function(){
    page=$(this).data('id')
    $('.selector').dialog('open');
  })
  $(".selector").dialog({
    resizable: false,
    autoOpen: false,
    height: 600,
    width: 700,
    title: "WHG Help",
    modal: true,
    buttons: { 'Close': function() {$(this).dialog('close');} },
    open: function(event, ui) {
      $('#helpme').load('/media/help/'+page+'.html');
    },
    show: {effect: "fade",duration: 400 },
    hide: {effect: "fade",duration: 400 }
  });
})
function updateTotals(dsid){
  $.get("/search/updatecounts",{ds_id: dsid},
    function(data){
      updates=data
      for(u in updates){
        html1='';html2='';html3='';
        <!--console.log(updates[u])-->
        html1+=updates[u]['pass1']
        html2+=updates[u]['pass2']
        html3+=updates[u]['pass3']
        $("#"+u+'_1').html(html1)
        $("#"+u+'_2').html(html2)
        $("#"+u+'_3').html(html3)
      }
    }
  )
}
$("[rel='tooltip']").tooltip();

$(".edit-name").click(function() {
  <!--$(".hidden").toggle()-->
  $(".editing-name").toggleClass("hidden")
  $(".form-name").toggleClass("hidden")
  $(".btn").toggleClass("hidden")
})
$(".edit-description").click(function() {
  <!--$(".hidden").toggle()-->
  $(".editing-description").toggleClass("hidden")
  $(".form-description").toggleClass("hidden")
  $(".btn").toggleClass("hidden")
})

$(".confirm-del-geoms").click(function(){
  return confirm('DANGER! Deletes all place_geom records created so far in Review step');
})
$(".confirm-del-all").click(function(){
  id=$(this).data('id')
  return confirm('DANGER! Destroys task, its hits, and clears matches confirmed in Review step...'+id);
})
