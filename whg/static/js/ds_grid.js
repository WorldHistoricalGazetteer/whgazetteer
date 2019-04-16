$(document).ready(function() {
  console.log('ds_grid.js loaded')
  var dt_table = $('.datatable').dataTable({
      language: dt_language,  // global variable defined in html
      order: [[ 0, "desc" ]],
      lengthMenu: [[25, 50, 100, 200], [25, 50, 100, 200]],
      columnDefs: [
          {orderable: true,
           searchable: true,
           className: "center",
           targets: [0, 1, 2, 3]
          },
          {
              name: 'place_id',
              targets: [0]
          },
          {
              name: 'src_id',
              targets: [1]
          },
          {
              name: 'title',
              targets: [2]
          },
          {
              name: 'ccodes',
              targets: [3]
          }
      ],
      select: { style: 'single'},
      searching: true,
      processing: true,
      serverSide: true,
      stateSave: true,
      ajax: DS_LIST_URL
  });
});
