-- commands to poulate WHG instance with sufficient datasets (empty) and collections to display home page
-- to be run after 'createsuperuser', as these will be owned by that user.
-- WARNING: removes ALL existing datasets and collections

-- DELETE ANY COLLECTIONS, reset sequence
delete from public.collections_datasets;
delete from public.collections;
ALTER SEQUENCE collections_id_seq RESTART WITH 1;

-- DELETE ANY DATASETS; reset sequence
DELETE FROM public."dataset_file";
DELETE FROM public."datasets";
ALTER SEQUENCE datasets_id_seq RESTART WITH 1;

-- CREATE 2 DATASETS (ids 1 & 2)
with z as (select * from users_user au where is_superuser = true)
	INSERT INTO public."datasets" (owner_id, label, title, description, core, public, 
		datatype, featured, image_file, create_date) VALUES
		((select id from z), 'sample01','Sample dataset 01','sample dataset required to load home page', 
			false, false, 'place', 1, 'dummy_image.png', now()),
		((select id from z), 'sample02','Sample dataset 02','sample dataset required to load home page', 
			false, false, 'place', 2, 'dummy_image.png', now());

-- add dataset_file pointers to dummy file
INSERT INTO public."dataset_file" (dataset_id_id, file, rev, format, datatype) VALUES
	(1, 'dummy_file.txt', 1, 'delimited', 'place'),
	(2, 'dummy_file.txt', 1, 'delimited', 'place');

-- CREATE COLLECTIONS
with z as (select * from users_user au where is_superuser = true)
	INSERT INTO public."collections" (owner_id, title, description, collection_class, 
		featured, omitted, status, image_file, created, keywords, public, submitted, nominated) VALUES
		((select id from z), 'Sample Collection 01', 'Sample for loading home page', 'dataset', 
			1, '{}', 'sandbox', 'dummy_image.png', now(), array['could','it','be'], FALSE, FALSE, FALSE ),
		((select id from z), 'Sample Collection 02', 'Sample for loading home page', 'dataset', 
			2, '{}', 'sandbox', 'dummy_image.png', now(), array['so','it','goes'], FALSE, FALSE, FALSE ) ;

INSERT INTO public."collections_datasets" (collection_id, dataset_id) VALUES
	(1, 1),	(1, 2),	(2, 1),	(2, 2);
