package auth

import (
	"context"
	"database/sql"
	"oss/global"
	pa "oss/proto/auth"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	AuthDBFileName    string
	SuperUserName     string
	SuperUserPassword string
	JWTSecretKey      string
}

type AuthServer struct {
	root   string
	config *Config
	db     *sql.DB
}

func NewAuthServer(root string, config *Config) *AuthServer {
	authServer := &AuthServer{
		root:   root,
		config: config,
	}
	err := authServer.start()
	if err != nil {
		logrus.WithError(err).Fatal("Initialize authentication server failed")
	}
	return authServer
}

func (s *AuthServer) Login(ctx context.Context, request *pa.LoginRequest) (*pa.LoginResponse, error) {
	token := s.generateToken(request.Name, request.Pass)
	if len(token) == 0 {
		return nil, status.Error(codes.Unauthenticated, "authentication failed")
	}
	return &pa.LoginResponse{
		Token: token,
	}, nil
}

func (s *AuthServer) Grant(ctx context.Context, request *pa.GrantRequest) (*pa.GrantResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if role != global.RoleAdmin {
		if !s.checkGrantPermission(performer, request.Name, request.Bucket) {
			return nil, status.Error(codes.PermissionDenied, "only admin or bucket owner can grant other users")
		}
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

func (s *AuthServer) Check(ctx context.Context, request *pa.CheckRequest) (*pa.CheckResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if role == global.RoleAdmin || request.Permission == global.PermissionNone {
		return &pa.CheckResponse{}, nil
	}

	if granted := s.checkActionPermission(performer, role, request.Bucket, int(request.Permission)); granted {
		return &pa.CheckResponse{}, nil
	}
	return nil, status.Error(codes.PermissionDenied, "insufficient permission level")
}

func (s *AuthServer) Register(ctx context.Context, request *pa.RegisterRequest) (*pa.RegisterResponse, error) {
	performer, role := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if role != global.RoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "only admin can register user")
	}
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

func (s *AuthServer) Confirm(ctx context.Context, request *pa.ConfirmRequest) (*pa.ConfirmResponse, error) {
	performer, _ := s.parseToken(request.Token)
	if len(performer) == 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	if !s.addPermission(performer, request.Bucket, global.PermissionOwner) {
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	return &pa.ConfirmResponse{}, nil
}

func (s *AuthServer) Clear(ctx context.Context, request *pa.ClearRequest) (*pa.ClearResponse, error) {
	_, err := s.db.Exec(`selete from privilege where bucket=?`, request.Bucket)
	if err != nil {
		logrus.WithError(err).Error("Delete table content falied")
		return nil, status.Error(codes.Internal, "authentication database failed")
	}
	return &pa.ClearResponse{}, nil
}
