package candidate

// leader:<appName>:current
// leader:<appName>:candidates
type KeyGenerator struct{}

func (g *KeyGenerator) GetLeaderKey(app string) string {
	return "leader:" + app + ":current"
}

func (g *KeyGenerator) GetLeaderCandidatesKey(app string) string {
	return "leader:" + app + ":candidates"
}
