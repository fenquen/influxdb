// Package meta provides control over meta data for InfluxDB,
// such as controlling databases, retention policies, users, etc.
package meta

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/logger"
	influxdb "github.com/influxdata/influxdb/v2/v1"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

	// Filename specifies the default name of the metadata file.
	Filename = "meta.db"

	// ShardGroupDeletedExpiration is the amount of time before a shard group info will be removed from cached
	// data after it has been marked deleted (2 weeks).
	ShardGroupDeletedExpiration = -2 * 7 * 24 * time.Hour
)

// Name of the bucket to store TSM metadata
var (
	BucketName  = []byte("v1_tsm1_metadata")
	metadataKey = []byte(Filename)
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")
)

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *zap.Logger

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	// Authentication cache.
	authCache map[string]authUser

	store kv.Store

	retentionAutoCreate bool
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *Client.
func NewClient(config *Config, store kv.Store) *Client {
	return &Client{
		cacheData: &Data{
			ClusterID: uint64(rand.Int63()),
			Index:     1,
		},
		closing:             make(chan struct{}),
		changed:             make(chan struct{}),
		logger:              zap.NewNop(),
		authCache:           make(map[string]authUser),
		store:               store,
		retentionAutoCreate: config.RetentionAutoCreate,
	}
}

// Open a connection to a meta service cluster.
func (client *Client) Open() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	// Try to load from disk
	if err := client.Load(); err != nil {
		return err
	}

	// If this is a brand new instance, persist to disk immediately.
	if client.cacheData.Index == 1 {
		if err := snapshot(client.store, client.cacheData); err != nil {
			return err
		}
	}

	return nil
}

// Close the meta service cluster connection.
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-client.closing:
		return nil
	default:
		close(client.closing)
	}

	return nil
}

// Database returns info for the requested database.
func (client *Client) Database(name string) *DatabaseInfo {
	client.mu.RLock()
	defer client.mu.RUnlock()

	for _, d := range client.cacheData.Databases {
		if d.Name == name {
			return &d
		}
	}

	return nil
}

// Databases returns a list of all database infos.
func (client *Client) Databases() []DatabaseInfo {
	client.mu.RLock()
	defer client.mu.RUnlock()

	dbs := client.cacheData.Databases
	if dbs == nil {
		return []DatabaseInfo{}
	}
	return dbs
}

// CreateDatabase creates a database or returns it if it already exists.
func (client *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if db := data.Database(name); db != nil {
		return db, nil
	}

	if err := data.CreateDatabase(name); err != nil {
		return nil, err
	}

	// create default retention policy
	if client.retentionAutoCreate {
		rpi := DefaultRetentionPolicyInfo()
		if err := data.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	}

	db := data.Database(name)

	if err := client.commit(data); err != nil {
		return nil, err
	}

	return db, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified
// retention policy.
//
// When creating a database with a retention policy, the retention policy will
// always be set to default. Therefore if the caller provides a retention policy
// that already exists on the database, but that retention policy is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// retention policy, and that retention policy is already the default for the
// database.
func (client *Client) CreateDatabaseWithRetentionPolicy(name string, spec *RetentionPolicySpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	db := data.Database(name)
	if db == nil {
		if err := data.CreateDatabase(name); err != nil {
			return nil, err
		}
		db = data.Database(name)
	}

	// No existing retention policies, so we can create the provided policy as
	// the new default policy.
	rpi := spec.NewRetentionPolicyInfo()
	if len(db.RetentionPolicies) == 0 {
		if err := data.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	} else if !spec.Matches(db.RetentionPolicy(rpi.Name)) {
		// In this case we already have a retention policy on the database and
		// the provided retention policy does not match it. Therefore, this call
		// is not idempotent and we need to return an error.
		return nil, ErrRetentionPolicyConflict
	}

	// If a non-default retention policy was passed in that already exists then
	// it's an error regardless of if the exact same retention policy is
	// provided. CREATE DATABASE WITH RETENTION POLICY should only be used to
	// create DEFAULT retention policies.
	if db.DefaultRetentionPolicy != rpi.Name {
		return nil, ErrRetentionPolicyConflict
	}

	// Commit the changes.
	if err := client.commit(data); err != nil {
		return nil, err
	}

	// Refresh the database info.
	db = data.Database(name)

	return db, nil
}

// DropDatabase deletes a database.
//
// Returns nil if no database exists
func (client *Client) DropDatabase(name string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DropDatabase(name); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (client *Client) CreateRetentionPolicy(database string, spec *RetentionPolicySpec, makeDefault bool) (*RetentionPolicyInfo, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	rp := spec.NewRetentionPolicyInfo()
	if err := data.CreateRetentionPolicy(database, rp, makeDefault); err != nil {
		return nil, err
	}

	if err := client.commit(data); err != nil {
		return nil, err
	}

	return rp, nil
}

// RetentionPolicy returns the requested retention policy info.
func (client *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	db := client.cacheData.Database(database)
	if db == nil {
		return nil, influxdb.ErrDatabaseNotFound(database)
	}

	return db.RetentionPolicy(name), nil
}

// DropRetentionPolicy drops a retention policy from a database.
func (client *Client) DropRetentionPolicy(database, name string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DropRetentionPolicy(database, name); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// UpdateRetentionPolicy updates a retention policy.
func (client *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.UpdateRetentionPolicy(database, name, rpu, makeDefault); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// Users returns a slice of UserInfo representing the currently known users.
func (client *Client) Users() []UserInfo {
	client.mu.RLock()
	defer client.mu.RUnlock()

	users := client.cacheData.Users

	if users == nil {
		return []UserInfo{}
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (client *Client) User(name string) (User, error) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	for _, u := range client.cacheData.Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, ErrUserNotFound
}

// bcryptCost is the cost associated with generating password with bcrypt.
// This setting is lowered during testing to improve test suite performance.
var bcryptCost = bcrypt.DefaultCost

// hashWithSalt returns a salted hash of password using salt.
func (client *Client) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (client *Client) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, client.hashWithSalt(salt, password), nil
}

// CreateUser adds a user with the given name and password and admin status.
func (client *Client) CreateUser(name, password string, admin bool) (User, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	// See if the user already exists.
	if u := data.user(name); u != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil || u.Admin != admin {
			return nil, ErrUserExists
		}
		return u, nil
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, err
	}

	if err := data.CreateUser(name, string(hash), admin); err != nil {
		return nil, err
	}

	u := data.user(name)

	if err := client.commit(data); err != nil {
		return nil, err
	}

	return u, nil
}

// UpdateUser updates the password of an existing user.
func (client *Client) UpdateUser(name, password string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	if err := data.UpdateUser(name, string(hash)); err != nil {
		return err
	}

	delete(client.authCache, name)

	return client.commit(data)
}

// DropUser removes the user with the given name.
func (client *Client) DropUser(name string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DropUser(name); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// SetPrivilege sets a privilege for the given user on the given database.
func (client *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.SetPrivilege(username, database, p); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (client *Client) SetAdminPrivilege(username string, admin bool) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.SetAdminPrivilege(username, admin); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (client *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	p, err := client.cacheData.UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (client *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	p, err := client.cacheData.UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (client *Client) AdminUserExists() bool {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.cacheData.AdminUserExists()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (client *Client) Authenticate(username, password string) (User, error) {
	// Find user.
	client.mu.RLock()
	userInfo := client.cacheData.user(username)
	client.mu.RUnlock()
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Check the local auth cache first.
	client.mu.RLock()
	au, ok := client.authCache[username]
	client.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(client.hashWithSalt(au.salt, password), au.hash) {
			return userInfo, nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}

	// generate a salt and hash of the password for the cache
	salt, hashed, err := client.saltedHash(password)
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	client.authCache[username] = authUser{salt: salt, hash: hashed, bhash: userInfo.Hash}
	client.mu.Unlock()
	return userInfo, nil
}

// UserCount returns the number of users stored.
func (client *Client) UserCount() int {
	client.mu.RLock()
	defer client.mu.RUnlock()

	return len(client.cacheData.Users)
}

// ShardIDs returns a list of all shard ids.
func (client *Client) ShardIDs() []uint64 {
	client.mu.RLock()

	var a []uint64
	for _, dbi := range client.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	client.mu.RUnlock()
	sort.Sort(uint64Slice(a))
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (client *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	// Find retention policy.
	rpi, err := client.cacheData.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (client *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error) {
	m := make(map[*ShardInfo]struct{})
	for _, mm := range sources.Measurements() {
		groups, err := client.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// DropShard deletes a shard by ID.
func (client *Client) DropShard(id uint64) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()
	data.DropShard(id)
	return client.commit(data)
}

// TruncateShardGroups truncates any shard group that could contain timestamps beyond t.
func (client *Client) TruncateShardGroups(t time.Time) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()
	data.TruncateShardGroups(t)
	return client.commit(data)
}

// PruneShardGroups remove deleted shard groups from the data store.
func (client *Client) PruneShardGroups() error {
	var changed bool
	expiration := time.Now().Add(ShardGroupDeletedExpiration)
	client.mu.Lock()
	defer client.mu.Unlock()
	data := client.cacheData.Clone()
	for i, d := range data.Databases {
		for j, rp := range d.RetentionPolicies {
			var remainingShardGroups []ShardGroupInfo
			for _, sgi := range rp.ShardGroups {
				if sgi.DeletedAt.IsZero() || !expiration.After(sgi.DeletedAt) {
					remainingShardGroups = append(remainingShardGroups, sgi)
					continue
				}
				changed = true
			}
			data.Databases[i].RetentionPolicies[j].ShardGroups = remainingShardGroups
		}
	}
	if changed {
		return client.commit(data)
	}
	return nil
}

// CreateShardGroupWithShards creates a shard group on a database and policy for a given timestamp and assign shards to the shard group
func (client *Client) CreateShardGroupWithShards(database, policy string, timestamp time.Time, shards []ShardInfo) (*ShardGroupInfo, error) {
	// Check under a read-lock
	client.mu.RLock()
	if sg, _ := client.cacheData.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		client.mu.RUnlock()
		return sg, nil
	}
	client.mu.RUnlock()

	client.mu.Lock()
	defer client.mu.Unlock()

	// Check again under the write lock
	data := client.cacheData.Clone()
	if sg, _ := data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}

	sgi, err := createShardGroup(data, database, policy, timestamp, shards...)
	if err != nil {
		return nil, err
	}

	if err := client.commit(data); err != nil {
		return nil, err
	}

	return sgi, nil
}

func (client *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	return client.CreateShardGroupWithShards(database, policy, timestamp, nil)
}

func createShardGroup(data *Data, database, policy string, timestamp time.Time, shards ...ShardInfo) (*ShardGroupInfo, error) {
	// It is the responsibility of the caller to check if it exists before calling this method.
	if sg, _ := data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return nil, ErrShardGroupExists
	}

	if err := data.CreateShardGroup(database, policy, timestamp, shards...); err != nil {
		return nil, err
	}

	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}

	sgi := rpi.ShardGroupByTimestamp(timestamp)
	return sgi, nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (client *Client) DeleteShardGroup(database, policy string, id uint64) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DeleteShardGroup(database, policy, id); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (client *Client) PrecreateShardGroups(from, to time.Time) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	data := client.cacheData.Clone()
	var changed bool

	for _, databaseInfo := range data.Databases {
		for _, retentionPolicy := range databaseInfo.RetentionPolicies {
			if len(retentionPolicy.ShardGroups) == 0 {
				// No data was ever written to this group, or all groups have been deleted.
				continue
			}
			shardGroupInfo := retentionPolicy.ShardGroups[len(retentionPolicy.ShardGroups)-1] // Get the last group in time.
			if !shardGroupInfo.Deleted() && shardGroupInfo.EndTime.Before(to) && shardGroupInfo.EndTime.After(from) {
				// Group is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive shard group.
				nextShardGroupTime := shardGroupInfo.EndTime.Add(1 * time.Nanosecond)
				// if it already exists, continue
				if sg, _ := data.ShardGroupByTimestamp(databaseInfo.Name, retentionPolicy.Name, nextShardGroupTime); sg != nil {
					client.logger.Info("Shard group already exists",
						logger.ShardGroup(sg.ID),
						logger.Database(databaseInfo.Name),
						logger.RetentionPolicy(retentionPolicy.Name))
					continue
				}
				newGroup, err := createShardGroup(data, databaseInfo.Name, retentionPolicy.Name, nextShardGroupTime)
				if err != nil {
					client.logger.Info("Failed to precreate successive shard group",
						zap.Uint64("group_id", shardGroupInfo.ID), zap.Error(err))
					continue
				}
				changed = true
				client.logger.Info("New shard group successfully precreated",
					logger.ShardGroup(newGroup.ID),
					logger.Database(databaseInfo.Name),
					logger.RetentionPolicy(retentionPolicy.Name))
			}
		}
	}

	if changed {
		if err := client.commit(data); err != nil {
			return err
		}
	}

	return nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (client *Client) ShardOwner(shardID uint64) (database, policy string, sgi *ShardGroupInfo) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	for _, dbi := range client.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, g := range rpi.ShardGroups {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						policy = rpi.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// CreateContinuousQuery saves a continuous query with the given name for the given database.
func (client *Client) CreateContinuousQuery(database, name, query string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.CreateContinuousQuery(database, name, query); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (client *Client) DropContinuousQuery(database, name string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DropContinuousQuery(database, name); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (client *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.CreateSubscription(database, rp, name, mode, destinations); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (client *Client) DropSubscription(database, rp, name string) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	data := client.cacheData.Clone()

	if err := data.DropSubscription(database, rp, name); err != nil {
		return err
	}

	if err := client.commit(data); err != nil {
		return err
	}

	return nil
}

// SetData overwrites the underlying data in the meta store.
func (client *Client) SetData(data *Data) error {
	client.mu.Lock()

	d := data.Clone()

	if err := client.commit(d); err != nil {
		return err
	}

	client.mu.Unlock()

	return nil
}

// Data returns a clone of the underlying data in the meta store.
func (client *Client) Data() Data {
	client.mu.RLock()
	defer client.mu.RUnlock()
	d := client.cacheData.Clone()
	return *d
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (client *Client) WaitForDataChanged() chan struct{} {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.changed
}

// commit writes data to the underlying store.
// This method assumes c's mutex is already locked.
func (client *Client) commit(data *Data) error {
	data.Index++

	// try to write to disk before updating in memory
	if err := snapshot(client.store, data); err != nil {
		return err
	}

	// update in memory
	client.cacheData = data

	// close channels to signal changes
	close(client.changed)
	client.changed = make(chan struct{})

	return nil
}

// MarshalBinary returns a binary representation of the underlying data.
func (client *Client) MarshalBinary() ([]byte, error) {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the client.
func (client *Client) WithLogger(log *zap.Logger) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.logger = log.With(zap.String("service", "metaclient"))
}

// snapshot saves the current meta data to disk.
func snapshot(store kv.Store, data *Data) (err error) {
	var d []byte
	if d, err = data.MarshalBinary(); err != nil {
		return err
	}

	return store.Update(context.TODO(), func(tx kv.Tx) error {
		b, err := tx.Bucket(BucketName)
		if err != nil {
			return err
		}
		return b.Put(metadataKey, d)
	})
}

// Load loads the current meta data from disk.
func (client *Client) Load() error {
	return client.store.View(context.TODO(), func(tx kv.Tx) error {
		b, err := tx.Bucket(BucketName)
		if err != nil {
			return err
		}

		if data, err := b.Get(metadataKey); errors.Is(err, kv.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		} else {
			return client.cacheData.UnmarshalBinary(data)
		}
	})
}

func (client *Client) RLock() {
	client.store.RLock()
}

func (client *Client) RUnlock() {
	client.store.RUnlock()
}

func (client *Client) Backup(ctx context.Context, w io.Writer) error {
	return client.store.Backup(ctx, w)
}

func (client *Client) Restore(ctx context.Context, r io.Reader) error {
	if err := client.store.Restore(ctx, r); err != nil {
		return err
	}
	return client.Load()
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
