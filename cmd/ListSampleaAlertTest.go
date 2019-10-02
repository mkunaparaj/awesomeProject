package main

import (
	"fmt"
	"time"

	"github.com/sendgrid/mc-contacts/lib/listsample"
	m "github.com/sendgrid/mc-contacts-platform-tools/lib/migration_file"
)

type client struct {
	red  listsample.DAL
}

func main() {
	c := new()

	// put contacts into redis
	if err := c.putRedis(m.DirSnow); err != nil {
		fmt.Println("unable to put data into redis")
		return
	}
}

type config struct {
	RedisCredentials string `default:"localhost:6379"`
}

var cfg config

func (c *client) putRedis(dir string) error {

	// Redis Insertion

	// build batch put
	builder := listsample.NewListDeltaBatchBuilder()

	for i := 0; i < 1000000; i++ {

		builder.AddUpdate("Arthur Dent", "no list id", "no contact id", time.Now())
	}

	// run Batch put
	err := c.red.Put(builder.Build())
	if err != nil {
		fmt.Println("Error running batch put")
		return err
	}
	return nil
}

// new creates a new client for migration
func new() *client {

	c := &client{}

	// init redis
	clusterOptions := listsample.NewClusterOptions()
	clusterOptions.BoostrapHost = cfg.RedisCredentials
	r, err := listsample.NewDAL(listsample.WithClusterOptions(clusterOptions))
	if err != nil {
		panic(err)
	}
	c.red = r

	return c
}
