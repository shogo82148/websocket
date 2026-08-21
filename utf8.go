package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
)

type utf8State byte

const utf8StateFail utf8State = 8

// state transition table of the UTF-8 validator automaton
// ref. https://zenn.dev/mod_poppo/articles/utf8-validation
var utf8States = [...][256]utf8State{
	// 0: START
	{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 1
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 2
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 3
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 4
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 5
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 6
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 7
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 8
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 9
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // A
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // B
		utf8StateFail, utf8StateFail, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // C
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // D
		2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 3, // E
		5, 6, 6, 6, 7, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 1: TAILx1
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 8
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 9
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // A
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 2: A
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 8
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 9
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // A
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 3: TAILx2
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 8
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 9
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // A
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 4: B
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 8
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 9
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // A
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 5: C
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 8
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 9
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // A
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 6: TAILx3
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 8
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 9
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // A
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	// 7: D
	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 8
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 9
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // A
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},

	{
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 0
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 1
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 2
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 3
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 4
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 5
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 6
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 7
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 8
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // 9
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // A
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // B
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // C
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // D
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // E
		utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, utf8StateFail, // F
	},
}

// validateUTF8 advances the UTF-8 validation automaton from state by
// consuming p and returns the resulting state (or utf8StateFail).
func validateUTF8(state utf8State, p []byte) utf8State {
	if len(p) == 0 {
		return state
	}
	if len(p) == 1 {
		return utf8States[state][p[0]]
	}

	for len(p) > 8 {
		// skip over ASCII bytes quickly
		if state == 0 {
			bits := binary.LittleEndian.Uint64(p[:8])
			if bits&0x8080808080808080 == 0 {
				p = p[8:]
				continue
			}
			if bits&0x80808080 == 0 {
				p = p[4:]
				continue
			}
			if bits&0x8080 == 0 {
				p = p[2:]
				continue
			}
			if p[0] < 0x80 {
				p = p[1:]
				continue
			}
		}

		// consume the first byte and advance the state
		state = utf8States[state][p[0]]
		p = p[1:]
	}

	for len(p) > 0 {
		// skip over ASCII bytes quickly
		if state == 0 && p[0] < 0x80 {
			p = p[1:]
			continue
		}

		// consume the first byte and advance the state
		state = utf8States[state][p[0]]
		p = p[1:]
	}
	return state
}

// utf8Reader is a wrapper around an io.Reader that ensures that the data read
// from the underlying reader is valid UTF-8.
// If invalid UTF-8 is encountered, it returns an error.
type utf8Reader struct {
	ctx   context.Context
	r     io.Reader
	conn  *Conn
	state utf8State
}

// reset resets the utf8Reader to its initial state and sets the underlying reader.
func (r *utf8Reader) reset(ctx context.Context, reader io.Reader) {
	r.ctx = ctx
	r.r = reader
	r.state = 0
}

// Read reads data from the underlying reader and checks if it is valid UTF-8.
func (r *utf8Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.state = validateUTF8(r.state, p[:n])
	if r.state == utf8StateFail {
		r.conn.abnormalClosure(r.ctx, StatusInvalidFramePayloadData, "invalid UTF-8")
		return n, CloseError{
			Code:   StatusInvalidFramePayloadData,
			Reason: "invalid UTF-8",
		}
	}
	if errors.Is(err, io.EOF) && r.state != 0 {
		r.conn.abnormalClosure(r.ctx, StatusInvalidFramePayloadData, "invalid UTF-8")
		return n, CloseError{
			Code:   StatusInvalidFramePayloadData,
			Reason: "invalid UTF-8",
		}
	}
	return n, err
}

// utf8Writer is a wrapper around an io.Writer that ensures that the data written
// to the underlying writer is valid UTF-8.
// If invalid UTF-8 is encountered, it returns an error.
type utf8Writer struct {
	ctx   context.Context
	w     io.WriteCloser
	conn  *Conn
	state utf8State
}

// reset resets the utf8Writer to its initial state and sets the underlying writer.
func (w *utf8Writer) reset(ctx context.Context, writer io.WriteCloser) {
	w.ctx = ctx
	w.w = writer
	w.state = 0
}

// Write writes data to the underlying writer and checks if it is valid UTF-8.
func (w *utf8Writer) Write(p []byte) (int, error) {
	w.state = validateUTF8(w.state, p)
	if w.state == utf8StateFail {
		w.conn.abnormalClosure(w.ctx, StatusInvalidFramePayloadData, "invalid UTF-8")
		return 0, CloseError{
			Code:   StatusInvalidFramePayloadData,
			Reason: "invalid UTF-8",
		}
	}
	return w.w.Write(p)
}

// Close closes the underlying writer.
func (w *utf8Writer) Close() error {
	var err error
	if w.state != 0 {
		w.conn.abnormalClosure(w.ctx, StatusInvalidFramePayloadData, "invalid UTF-8")
		err = CloseError{
			Code:   StatusInvalidFramePayloadData,
			Reason: "invalid UTF-8",
		}
	}
	if err0 := w.w.Close(); err == nil {
		err = err0
	}
	return err
}
