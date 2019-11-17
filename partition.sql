ALTER TABLE logs add
  PARTITION (inserted_at = '2019/11/13/08') LOCATION 's3://serverless-db-bucket-1975017/2019/11/13/08/';