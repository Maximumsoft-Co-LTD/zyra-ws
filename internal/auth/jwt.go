// Package auth validates the same JWT tokens that zyra-api issues.
// Both services share the same tokenKey env var.
package auth

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrTokenInvalid = errors.New("token invalid or expired")
	ErrTokenMissing = errors.New("token missing")
)

// Claims mirrors the fields zyra-api encodes in access tokens.
type Claims struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role_"`
}

// ValidateToken parses and validates a signed JWT string.
// Returns extracted claims on success.
func ValidateToken(tokenStr, signingKey string) (Claims, error) {
	if tokenStr == "" {
		return Claims{}, ErrTokenMissing
	}

	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(signingKey), nil
	})
	if err != nil || !token.Valid {
		return Claims{}, ErrTokenInvalid
	}

	mc, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return Claims{}, ErrTokenInvalid
	}

	str := func(key string) string {
		v, _ := mc[key].(string)
		return v
	}

	name := str("name")
	if last := str("lastname"); last != "" {
		name = name + " " + last
	}
	if dn := str("display_name"); dn != "" {
		name = dn
	}

	return Claims{
		UserID:      str("id"),
		Username:    str("username"),
		DisplayName: name,
		Role:        str("role_"),
	}, nil
}
