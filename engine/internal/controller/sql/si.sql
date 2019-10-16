
-- SI2 Engines : https://docs.google.com/document/d/1ggA2CGIfGkDa4k82cCizqHc-5HRUZePG5QLVJCbHexo/edit#heading=h.tuki60nker41

-- SI2 playback segment creator
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'352556c7-de07-4d55-b33f-74b1cf237f25',E'SI2 Playback segment creator',E'Engine used for creating playback segments from an input stream into the TDO.  Should be for initial ingestion only',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'352556c7-de07-4d55-b33f-74b1cf237f25',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id) 
DO NOTHING;

-- SI audio/video chunk creator
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'8bdb0e3b-ff28-4f6e-a3ba-887bd06e6440',E'SI2 audio/video Chunk creator',E'Engine used for splitting a stream to audio or video',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'8bdb0e3b-ff28-4f6e-a3ba-887bd06e6440',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id) 
DO NOTHING;

-- SI frame chunk creator
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'a804d90e-223b-48a3-922b-c334e3a2f267',E'SI2 frame chunk creator',E'Engine used for splitting a stream to frames',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'a804d90e-223b-48a3-922b-c334e3a2f267',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id) 
DO NOTHING;

-- SI2 Stream Filter
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'5fcd4ef5-0b45-4dad-a606-15261e91d0d2',E'SI2 Stream filter',E'Engine used for transcoding stream from one format to another',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'5fcd4ef5-0b45-4dad-a606-15261e91d0d2',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id) 
DO NOTHING;

-- SI2 Stream Asset Creator
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'75fc943b-b5b0-4fe1-bcb6-9a7e1884257a',E'SI2 Stream Asset Creator',E'Engine used for ingesting a stream and storing the data as an asset in the TDO with the given assetType',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'75fc943b-b5b0-4fe1-bcb6-9a7e1884257a',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id)
DO NOTHING;


-- SI2 Chunk Transcoder
INSERT INTO "job_new"."engine"("engine_id","engine_name","engine_description","engine_category_id","engine_state","deployment_model","owner_organization_id","is_public","price","rating","website","logo_path","order","dependency","core_job_data","fields","validation","application_id","asset","creates_recording","deleted","library_required","icon_path","engine_currency","engine_alias_id","engine_alias_name","engine_alias_description","engine_alias_logo_path","jwt_rights","use_cases","industries","engine_manifest")
VALUES
(E'674977da-f28e-4d73-a0f9-3e6b0cf21231',E'SI2 Chunk Transcoder',E'Engine used for transcoding chunks from one format to another',E'925e8039-5246-4ced-9f2b-b456d0b57ea1',E'pending',0,7682,TRUE,NULL,NULL,NULL,E'https://www.filepicker.io/api/file/p2bLdy4nRrOwv34yoEhg',NULL,NULL,E'{"category": "ingestion"}',E'[]',NULL,NULL,NULL,TRUE,FALSE,FALSE,NULL,E'USD',E'674977da-f28e-4d73-a0f9-3e6b0cf21231',NULL,NULL,NULL,NULL,NULL,NULL,NULL)
ON CONFLICT (engine_id)
DO NOTHING;