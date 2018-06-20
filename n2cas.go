package n2cas

import (
	"time"

	"io/ioutil"

	"github.com/gocql/gocql"
	"gopkg.in/yaml.v2"
)

// Cassandra wrapper
type CassandraCfgFile struct {
	Cfg CassandraCfg `yaml:"cassandra"`
}

// Cassandra wrapper
type CassandraCfg struct {
	Cluster  []string `yaml:"cluster"`
	Keyspace string   `yaml:"keyspace"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	NumConns int      `yaml:"numConns"`
}

// cas defines a Cassandra session and
// it's configuration.
type cas struct {
	cfg     CassandraCfg
	Session *gocql.Session
}

// CasRows used for simple result sets not bound to a
// specific type
type CasRows []map[string]interface{}

// Query basic query wrapper for generic data returned as CasRows
func (c *cas) Query(query string) (CasRows, error) {
	// call external libs for business logic here
	q := c.Session.Query(query)
	err := q.Exec()
	if err != nil {
		return nil, err
	}

	itr := q.Iter()
	defer itr.Close()

	ret := make(CasRows, 0)

	for {
		// New map each iteration
		row := make(map[string]interface{})
		if !itr.MapScan(row) {
			break
		}
		ret = append(ret, row)
	}

	if err := itr.Close(); err != nil {
		return nil, err
	}

	return ret, nil
}

// NewCassandraFromYaml creates a cassandra object with
func NewCassandraFromYaml(path string) (*cas, error) {
	ymlData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	casCfgFile := CassandraCfgFile{}

	err = yaml.Unmarshal([]byte(ymlData), &casCfgFile)
	if err != nil {
		return nil, err
	}

	return Cassandra(casCfgFile.Cfg)
}

// Cassandra produces a cassandra object with session
func Cassandra(cfg CassandraCfg) (*cas, error) {

	cluster := gocql.NewCluster(cfg.Cluster...)
	cluster.DisableInitialHostLookup = true
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3}
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 10 * time.Second

	if cfg.Username != "" && cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	cluster.Keyspace = cfg.Keyspace
	cluster.NumConns = cfg.NumConns

	session, err := cluster.CreateSession()

	return &cas{cfg: cfg, Session: session}, err
}
