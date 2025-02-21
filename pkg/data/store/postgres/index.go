package postgres

const createIndexesSQL = `
-- Add indexes for improved query performance
CREATE INDEX IF NOT EXISTS idx_entities_type ON Entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_parent_id ON Entities(parent_id);

-- Add indexes for notification queries
CREATE INDEX IF NOT EXISTS idx_notif_config_entity_field ON NotificationConfigEntityId(entity_id, field_name);
CREATE INDEX IF NOT EXISTS idx_notif_config_type_field ON NotificationConfigEntityType(entity_type, field_name);

-- Remove old index for unacknowledged notifications and add simpler index
CREATE INDEX IF NOT EXISTS idx_notifications_service ON Notifications(service_id);

-- Add index for notification timestamps
CREATE INDEX IF NOT EXISTS idx_notifications_timestamp ON Notifications(timestamp);
`
