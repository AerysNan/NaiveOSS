package auth

import (
	"context"
	"database/sql"
	"oss/global"
	pa "oss/proto/auth"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config for authentication server
type Config struct {
	AuthDBFileName    string
	SuperUserName     string
	SuperUserPassword string
	JWTSecretKey      string
}

// Server for authentication server struct
type Server struct {
	mu     sync.RWMutex
	root   string
	config *Config
	db     *sql.DB
}

// NewAuthServer returns a new authentication server
func NewAuthServer(root string, config *Config) *Server {
	authServer := &Server{
		mu:     sync.RWMutex{},
		root:   root,
		config: config,
	}
	err := authServer.start()
	if err != nil {
		logrus.WithError(err).Fatal("Initialize authentication server failed")
	}
	return authServer
}

// Login handles user login request
func (s *Server) Login(ctx context.Context, request *pa.LoginRequest) (*pa.LoginResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	token := s.generateToken(request.Name, request.Pass)
	if len(token) == 0 {
		return nil, status.Error(codes.Unauthenticated, "authentication failed")
	}
	return &pa.LoginResponse{
		Token: token,
	}, nil
}

// Grant handles user permission granting
func (s *Server) Grant(ctx context.Context, request *pa.GrantRequest) (*pa.GrantResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if role != global.RoleAdmin && !s.checkGrantPermission(performer, request.Name, request.Bucket) {
		return nil, status.Error(codes.PermissionDenied, "only admin or bucket owner can grant other users")
	}
	if request.Permission < global.PermissionNone || request.Permission > global.PermissionOwner {
		return nil, status.Error(codes.InvalidArgument, "no such permission level")
	}
	success := s.addPermission(request.Name, request.Bucket, int(request.Permission))
	if !success {
		return nil, status.Error(codes.Internal, "set permission level failed")
	}
	return &pa.GrantResponse{}, nil
}

// Check checks if a user has enough permission level to execute certain command
func (s *Server) Check(ctx context.Context, request *pa.CheckRequest) (*pa.CheckResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if role == global.RoleAdmin || request.Permission == global.PermissionNone {
		return &pa.CheckResponse{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if granted := s.checkActionPermission(performer, role, request.Bucket, int(request.Permission)); granted {
		return &pa.CheckResponse{}, nil
	}
	return nil, status.Error(codes.PermissionDenied, "insufficient permission level")
}

// Register handles new user creation
func (s *Server) Register(ctx context.Context, request *pa.RegisterRequest) (*pa.RegisterResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if role != global.RoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "only admin can register user")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.checkUserCreation(request.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	if !ok {
		return nil, status.Error(codes.AlreadyExists, "user already exists")
	}
	if request.Role < global.RoleUser || request.Role > global.RoleAdmin {
		return nil, status.Error(codes.InvalidArgument, "invalid role valid")
	}
	ok = s.createUser(request.Name, request.Pass, int(request.Role))
	if !ok {
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	return &pa.RegisterResponse{}, nil
}

// Confirm grants a user with owner permission after bucket creation
func (s *Server) Confirm(ctx context.Context, request *pa.ConfirmRequest) (*pa.ConfirmResponse, error) {
	performer, _ := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.addPermission(performer, request.Bucket, global.PermissionOwner) {
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	return &pa.ConfirmResponse{}, nil
}

// Clear clears all privileges associated with a deleted bucket
func (s *Server) Clear(ctx context.Context, request *pa.ClearRequest) (*pa.ClearResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`delete from privilege where bucket=?`, request.Bucket)
	if err != nil {
		logrus.WithError(err).Error("Delete table content failed")
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	return &pa.ClearResponse{}, nil
}
