--drop table if exists STV2023081256__STAGING.group_log cascade;

create table STV2023081256__STAGING.group_log (
  group_id integer, 
  user_id integer, 
  user_id_from integer, 
  event varchar(10), 
  datetime timestamp
) 
order by 
  group_id, 
  user_id SEGMENTED BY hash(group_id) all nodes PARTITION BY datetime :: date 
GROUP BY 
  calendar_hierarchy_day(datetime :: date, 3, 2);
ALTER TABLE 
  STV2023081256__STAGING.group_log 
ADD 
  CONSTRAINT fk_group_log_groups_group_id FOREIGN KEY (group_id) references STV2023081256__STAGING.groups (id);
ALTER TABLE 
  STV2023081256__STAGING.group_log 
ADD 
  CONSTRAINT fk_group_log_users_user_id_from FOREIGN KEY (user_id) references STV2023081256__STAGING.users (id);
ALTER TABLE 
  STV2023081256__STAGING.group_log 
ADD 
  CONSTRAINT fk_group_log_users_user_id FOREIGN KEY (user_id_from) references STV2023081256__STAGING.users (id);
