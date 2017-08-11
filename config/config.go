// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period time.Duration `config:"period"`
	Host string `config:"host"`
	Port string `config:"port"`
	Cluster string `config:"cluster"` 
	Groups []string `config:"groups"`
}

var DefaultConfig = Config{
	Period: 10 * time.Second,
	Host: "bklise.goomzee.com",
	Port: "8000",
	Cluster: "local",
	Groups: []string{},
}
