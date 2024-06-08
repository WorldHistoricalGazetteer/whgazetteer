copy (
select id, place_id, name_src_id, src_id, jsonb, task_id, toponym
	from place_name p 
	order by place_id;
) TO '/home/whgadmin/sites/data_dumps/exported_placename_20240525.csv' WITH CSV HEADER;
