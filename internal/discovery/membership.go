package discovery

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"net"
)

// Membership type-wraps Serf to provide discovery and
// cluster membership to the service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// New creates a Membership with the necessary config
// and event handler.
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// Config defines the typical config parameters for Serf
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// setupSerf creates and configures an instance of Serf,
// starting the eventsHandler goroutine to handle all events.
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handler represents a component in our service that
// needs to know when a server joins/leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// eventHandler runs a loop to read Serf events from
// the events channel, handling it according to the
// type. A node joining or leaving will result in Serf
// sending an event to all nodes, including the node
// that left or joined. If the event is for a local
// server, we return so the server doesn't act on itself.
// Cases range over event numbers because Serf will send
// one event if multiple nodes join simultaneously.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether a given Serf memeber is the local
// member by checking the members' names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns a point-in-time snapshot of the cluster'
// serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells the member th eleave the Serf cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs non-leader errors at the debug level. These
// logs are good candidates for removal.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	// m.logger.Error(msg, zap.Error(err), zap.String("name", member.Name), zap.String("rpc_addr",
	// 	member.Tags["rpc_addr"]))
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
