INSERT INTO datasets(id,label, title, description,core, public, datatype, owner_id) VALUES (1, 'Test dataset','Test dataset', 'Test dataset' ,True, True, 'test', 1);
INSERT INTO collections(id, title, description, keywords, public, owner_id) VALUES (1,'test','test',ARRAY['test'],TRUE,1); 
INSERT INTO collections_datasets(collection_id,dataset_id) VALUES (1,1);
