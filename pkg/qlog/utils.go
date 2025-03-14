package qlog

func Truncate(text string, width int) string {
	text = text[:min(len(text), width)]
	if len(text) == width {
		text = text + "..."
	}
	return text
}
