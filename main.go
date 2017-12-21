package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"github.com/zaibon/rkv/redis"
	"github.com/zaibon/rkv/storage"
)

func main() {

	addr := flag.String("addr", ":6379", "listen address")
	dir := flag.String("dir", "/tmp", "backend dir")
	debug := flag.Bool("debug", false, "enable debug logging")
	profileMode := flag.String("profile", "", "profile mode")

	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	switch *profileMode {
	case "cpu":
		defer profile.Start(profile.NoShutdownHook, profile.CPUProfile).Stop()
	case "mem":
		defer profile.Start(profile.NoShutdownHook, profile.MemProfile).Stop()
	case "trace":
		defer profile.Start(profile.NoShutdownHook, profile.TraceProfile).Stop()
	case "block":
		defer profile.Start(profile.NoShutdownHook, profile.BlockProfile).Stop()
	}

	// handle SIGINT
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	storage, err := storage.New(*dir)
	if err != nil {
		log.Fatalln(err)
	}
	defer storage.Close()
	server := redis.NewServer(storage)

	go server.Listen(*addr)

	// block until SIGINT is received
	<-c
	server.Close()
	log.Info("closing server")
}
