package discovery

import (
	"net"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"github.com/hashicorp/serf"
)

type Membership struct {
	Config
	handler Handler
	serf *serf.Serf
	events chan serf.Event
	logger *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {

}


type Config struct {

}

type Handler struct {

}