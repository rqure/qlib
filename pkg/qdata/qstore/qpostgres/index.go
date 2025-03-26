package qpostgres

const createIndexesSQL = `
-- Add indexes for improved query performance
CREATE INDEX IF NOT EXISTS idx_entities_type ON Entities(type);

-- EntitySchema indexes
CREATE INDEX IF NOT EXISTS idx_entityschema_entity_type ON EntitySchema(entity_type);
CREATE INDEX IF NOT EXISTS idx_entityschema_permissions ON EntitySchema USING GIN(read_permissions, write_permissions);

-- Field value table indexes
CREATE INDEX IF NOT EXISTS idx_strings_entity_id ON Strings(entity_id);
CREATE INDEX IF NOT EXISTS idx_binaryfiles_entity_id ON BinaryFiles(entity_id);
CREATE INDEX IF NOT EXISTS idx_ints_entity_id ON Ints(entity_id);
CREATE INDEX IF NOT EXISTS idx_floats_entity_id ON Floats(entity_id);
CREATE INDEX IF NOT EXISTS idx_bools_entity_id ON Bools(entity_id);
CREATE INDEX IF NOT EXISTS idx_entityreferences_entity_id ON EntityReferences(entity_id);
CREATE INDEX IF NOT EXISTS idx_timestamps_entity_id ON Timestamps(entity_id);
CREATE INDEX IF NOT EXISTS idx_choices_entity_id ON Choices(entity_id);
CREATE INDEX IF NOT EXISTS idx_entitylists_entity_id ON EntityLists(entity_id);

-- Reference tracking indexes
CREATE INDEX IF NOT EXISTS idx_entityreferences_field_value ON EntityReferences(field_value);
CREATE INDEX IF NOT EXISTS idx_entitylists_field_value ON EntityLists USING GIN(field_value);

-- Reverse references indexes (critical for entity relationship queries)
CREATE INDEX IF NOT EXISTS idx_reverseentityreferences_referenced_entity_id ON ReverseEntityReferences(referenced_entity_id);
CREATE INDEX IF NOT EXISTS idx_reverseentityreferences_referenced_by_entity_id ON ReverseEntityReferences(referenced_by_entity_id);
CREATE INDEX IF NOT EXISTS idx_reverseentityreferences_referenced_by_field_type ON ReverseEntityReferences(referenced_by_field_type);

-- Write time indexes for temporal queries
CREATE INDEX IF NOT EXISTS idx_strings_write_time ON Strings(write_time);
CREATE INDEX IF NOT EXISTS idx_entityreferences_write_time ON EntityReferences(write_time);
CREATE INDEX IF NOT EXISTS idx_timestamps_write_time ON Timestamps(write_time);
CREATE INDEX IF NOT EXISTS idx_timestamps_field_value ON Timestamps(field_value);

-- Choice option indexes
CREATE INDEX IF NOT EXISTS idx_choiceoptions_entity_type ON ChoiceOptions(entity_type);
`
