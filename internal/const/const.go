package message

const (
	ShortenURLsTopic = "shorten-urls"
	ShortenURLsGroup = "shorten-urls-group"
)

type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}
