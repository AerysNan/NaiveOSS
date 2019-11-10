package auth

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"oss/global"
	"path"

	"github.com/dgrijalva/jwt-go"
	"github.com/sirupsen/logrus"
)

func (s *AuthServer) start() error {
	var err error
	s.db, err = sql.Open("sqlite3", path.Join(s.root, s.config.AuthDBFileName))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`create table if not exists user (
			name text praimary key,
			pass text,
			role integer
		);`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`create table if not exists privilege (
			name text,
			bucket text,
			level integer,
			primary key(user, bucket));`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`insert or replace into user values (?, ?, ?);`,
		s.config.SuperUserName, fmt.Sprintf("%x", sha256.Sum256([]byte(s.config.SuperUserPassword))), 1)
	if err != nil {
		return err
	}
	return nil
}

func (s *AuthServer) generateToken(name string, pass string) string {
	result, err := s.db.Query(
		`select role from user where name=? and password=?`, name, pass)
	if err != nil {
		logrus.WithError(err).Error("Query table failed")
		return ""
	}
	defer result.Close()
	var role int
	if result.Next() {
		err = result.Scan(&role)
		if err != nil {
			logrus.WithError(err).Error("Scan query result failed")
			return ""
		}
		claim := jwt.MapClaims{
			"name": name,
			"role": role,
		}
		t := jwt.NewWithClaims(jwt.SigningMethodHS256, claim)
		token, err := t.SignedString(s.config.JWTSecretKey)
		if err != nil {
			logrus.WithError(err).Error("Sign JWT token failed")
			return ""
		}
		return token
	}
	return ""
}

func (s *AuthServer) parseToken(tokenString string) (string, int) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return []byte(s.config.JWTSecretKey), nil
	})
	if err != nil {
		logrus.WithError(err).Error("Parse JWT token failed")
		return "", -1
	}
	if !token.Valid {
		return "", -1
	}
	claim := token.Claims.(jwt.MapClaims)
	name, role := claim["name"].(string), claim["role"].(int)
	return name, role
}

func (s *AuthServer) addPermission(name string, bucket string, level int) bool {
	_, err := s.db.Exec(`insert or replace into privilege values (?, ?, ?)`, name, bucket, level)
	if err != nil {
		logrus.WithError(err).Error("Insert into table failed")
		return false
	}
	return true
}

func (s *AuthServer) checkGrantPermission(performer string, name string, bucket string) bool {
	result, err := s.db.Query(`select level from privilege where name=? and bucket=?`, performer, bucket)
	if err != nil {
		logrus.WithError(err).Error("Query table failed")
		return false
	}
	defer result.Close()
	var level int
	if result.Next() {
		err = result.Scan(&level)
		if err != nil {
			logrus.WithError(err).Error("Scan query result failed")
			return false
		}
		if level == global.PermissionOwner {
			return true
		}
	} else {
		return false
	}
	return false
}

func (s *AuthServer) checkActionPermission(performer string, role int, bucket string, permission int) bool {
	result, err := s.db.Query(`select level from privilege where name=? and bucket=?`, performer, bucket)
	if err != nil {
		logrus.WithError(err).Error("Query table failed")
		return false
	}
	defer result.Close()
	var level int
	if result.Next() {
		err = result.Scan(&result)
		if err != nil {
			logrus.WithError(err).Error("Scan query result failed")
			return false
		}
		return level >= permission
	}
	return false
}

func (s *AuthServer) checkUserCreation(name string) (bool, error) {
	result, err := s.db.Query(`select count(*) from user where name=?`, name)
	if err != nil {
		logrus.WithError(err).Error("Query table failed")
		return false, err
	}
	defer result.Close()
	var count int
	if result.Next() {
		err = result.Scan(&count)
		if err != nil {
			logrus.WithError(err).Error("Scan query result failed")
			return false, err
		}
		if count == 0 {
			return true, nil
		}
		return false, nil
	}
	return false, nil
}

func (s *AuthServer) createUser(name string, pass string, role int) bool {
	_, err := s.db.Exec(`insert into user values(?, ?, ?)`, name, pass, role)
	if err != nil {
		logrus.WithError(err).Error("Insert into table failed")
		return false
	}
	return true
}
