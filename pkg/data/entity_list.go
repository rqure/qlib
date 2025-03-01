package data

// EntityList represents a collection of entity references
type EntityList interface {
	// GetEntities returns all entity IDs in the list
	GetEntities() []string

	// SetEntities updates all entity IDs in the list
	SetEntities(entities []string) EntityList

	// Add adds an entity ID to the list if not already present
	// Returns true if added, false if already exists
	Add(entity string) bool

	// Remove removes an entity ID from the list
	// Returns true if removed, false if not found
	Remove(entity string) bool

	// Contains checks if an entity ID is in the list
	Contains(entity string) bool

	// Count returns the number of entities in the list
	Count() int

	// Clear removes all entities from the list
	Clear() EntityList
}
