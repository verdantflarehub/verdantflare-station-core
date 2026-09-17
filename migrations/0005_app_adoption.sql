ALTER TABLE station.app_operations DROP CONSTRAINT app_operations_action_check;
ALTER TABLE station.app_operations ADD CONSTRAINT app_operations_action_check CHECK (action IN ('install','start','stop','restart','delete','adopt'));
ALTER TABLE station.app_operations ADD COLUMN expected_workload_uid text NOT NULL DEFAULT '';
ALTER TABLE station.runtime_app_executions DROP CONSTRAINT runtime_app_executions_action_check;
ALTER TABLE station.runtime_app_executions ADD CONSTRAINT runtime_app_executions_action_check CHECK (action IN ('install','start','stop','restart','delete','adopt'));
