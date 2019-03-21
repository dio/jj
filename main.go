package main

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/credentials"

	"github.com/dio/jj/pkg/xds"
)

func main() {
	newADS := func(ctx context.Context, resp proto.Message) error {
		fmt.Println("OK!")
		return nil
	}
	loseContact := func(ctx context.Context) {}
	exitCleanup := func() {}
	waitc := make(chan struct{})
	creds, _ := credentials.NewClientTLSFromFile("/etc/ssl/certs/ca-certificates.crt", "")

	c := xds.NewXDSClient("ok", "ok", true, xds.Options{
		DialCreds: creds,
	}, newADS, loseContact, exitCleanup)
	c.Run()
	<-waitc
}
