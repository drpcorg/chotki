// Provides commone chotki errors definitions.
package chotki_errors

import "errors"

var (
	ErrObjectUnknown = errors.New("chotki: unknown object")
	ErrTypeUnknown   = errors.New("chotki: unknown object type")

	ErrFullscanIndexField                 = errors.New("chotki: field can't have fullscan index")
	ErrHashIndexFieldNotFirst             = errors.New("chotki: field can't have hash index if type is not FIRST")
	ErrHashIndexUinqueConstraintViolation = errors.New("chotki: hash index unique constraint violation")
	ErrBadHPacket                         = errors.New("chotki: bad handshake packet")
	ErrClosed                             = errors.New("chotki: no replica open")
)
