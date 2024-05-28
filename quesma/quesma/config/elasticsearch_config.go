package config

type ElasticsearchConfiguration struct {
	Url      *Url   `koanf:"url"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	Call     bool   `koanf:"call"`
	AdminUrl *Url   `koanf:"adminUrl"`
}
